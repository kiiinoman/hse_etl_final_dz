from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta, datetime as dt

SCHEMA = "ods"
COLLECTIONS = ["users", "products", "orders", "reviews"]
MONGO_CONN_ID = "mongo_default"
POSTGRES_CONN_ID = "postgres_default"


def detect_type(v):
    if isinstance(v, int):
        return "INTEGER"
    if isinstance(v, float):
        return "DOUBLE PRECISION"
    if isinstance(v, dt):
        return "TIMESTAMP"
    if isinstance(v, str):
        try:
            dt.fromisoformat(v)
            return "TIMESTAMP"
        except:
            return "TEXT"
    return "TEXT"


def parse_value(v):
    if isinstance(v, str):
        try:
            return dt.fromisoformat(v)
        except:
            return v
    return v


def load_collection(collection):
    mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    mongo_client = mongo_hook.get_conn()
    db = mongo_client["market"]
    docs = list(db[collection].find())
    if not docs:
        return
    columns = {}
    dict_fields = {}
    list_fields = []

    for doc in docs:
        for k, v in doc.items():
            if k == "_id":
                continue
            if isinstance(v, dict):
                if k not in dict_fields:
                    dict_fields[k] = {}
                for dk, dv in v.items():
                    t = detect_type(dv)
                    if dk not in dict_fields[k]:
                        dict_fields[k][dk] = t
                    elif dict_fields[k][dk] != t:
                        dict_fields[k][dk] = "TEXT"
            elif isinstance(v, list):
                if k not in list_fields:
                    list_fields.append(k)
            else:
                t = detect_type(v)
                if k not in columns:
                    columns[k] = t
                elif columns[k] != t:
                    columns[k] = "TEXT"

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

    cols_sql = ", ".join([f"{k} {v}" for k, v in columns.items()])
    cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{collection} CASCADE")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{collection}(
            pg_id SERIAL PRIMARY KEY,
            mongo_id TEXT,
            {cols_sql}
        )
    """)

    for field, subcols in dict_fields.items():
        sub_sql = ", ".join([f"{k} {v}" for k, v in subcols.items()])
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{collection}_{field}(
                pg_id SERIAL PRIMARY KEY,
                parent_id INT REFERENCES {SCHEMA}.{collection}(pg_id) ON DELETE CASCADE,
                {sub_sql}
            )
        """)

    for field in list_fields:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{collection}_{field}(
                pg_id SERIAL PRIMARY KEY,
                parent_id INT REFERENCES {SCHEMA}.{collection}(pg_id) ON DELETE CASCADE,
                {field} TEXT
            )
        """)

    pg_conn.commit()

    for doc in docs:
        main = {}
        dicts = {}
        lists = {}
        for k, v in doc.items():
            if k == "_id":
                continue
            if isinstance(v, dict):
                dicts[k] = v
            elif isinstance(v, list):
                lists[k] = v
            else:
                main[k] = parse_value(v)
        cols = ["mongo_id"] + list(main.keys())
        vals = [str(doc["_id"])] + list(main.values())
        placeholders = ",".join(["%s"] * len(vals))

        cursor.execute(
            f"""
            INSERT INTO {SCHEMA}.{collection} ({",".join(cols)})
            VALUES ({placeholders})
            RETURNING pg_id
            """,
            vals,
        )

        res = cursor.fetchone()

        if not res:
            continue

        parent_id = res[0]

        for field, d in dicts.items():
            cols = list(d.keys())
            vals = [parse_value(v) for v in d.values()]
            placeholders = ",".join(["%s"] * len(vals))
            cursor.execute(
                f"""
                INSERT INTO {SCHEMA}.{collection}_{field}
                (parent_id,{",".join(cols)})
                VALUES (%s,{placeholders})
                """,
                [parent_id] + vals,
            )

        for field, arr in lists.items():
            for value in arr:
                cursor.execute(
                    f"""
                    INSERT INTO {SCHEMA}.{collection}_{field}
                    (parent_id,{field})
                    VALUES (%s,%s)
                    """,
                    (parent_id, str(value)),
                )
    pg_conn.commit()
    cursor.close()


default_args = {
    "owner": "Alex",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="migration_mongo_psql",
    default_args=default_args,
    description="Миграция из mongo в psql",
    schedule="0 3 * * *",
    start_date=dt(2026, 2, 1),
    catchup=False,
    tags=["mongo", "postgres"],
) as dag:
    for col in COLLECTIONS:
        PythonOperator(
            task_id=f"load_{col}",
            python_callable=load_collection,
            op_args=[col],
        )
