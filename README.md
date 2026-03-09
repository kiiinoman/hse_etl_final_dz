# ETL_final_dz_Muhammadabbos_Zuyunov

---

## 1. Коллекции в mongoDB

### Users

**Поля:**
- `id` — уникальный идентификатор
- `name` — имя пользователя
- `email` — email
- `age` — возраст
- `address` — объект с полями: `street`, `city`, `zipcode`

**Пример документа:**
```json
{
  "id": 1,
  "name": "Alice Johnson",
  "email": "alice@example.com",
  "age": 34,
  "address": {
    "street": "123 Main St",
    "city": "Dallas",
    "zipcode": "75201"
  }
}
```
### orders

**Поля:**
- `user_id` — id пользователя (`users.id`)
- `product_ids` — массив id товаров (`products.id`)
- `total` — сумма заказа
- `status` — статус (pending, shipped, delivered)
- `order_time` — дата и время заказа

**Пример документа:**
```json
{
  "user_id": 5,
  "product_ids": [12, 45, 78],
  "total": 799.99,
  "status": "shipped",
  "order_time": "2024-03-09T14:25:00"
}
```
### products

**Поля:**
- `id` — уникальный идентификатор товара
- `name` — название товара
- `category` — категория (Electronics, Books, Clothing, Toys)
- `price` — цена
- `tags` — массив тегов

**Пример документа:**
```json
{
  "id": 1,
  "name": "Laptop",
  "category": "Electronics",
  "price": 499.99,
  "tags": ["tech", "computer", "portable"]
}
```
### reviews

**Поля:**
- `product_id` — id товара (`products.id`)
- `user_id` — id пользователя (`users.id`)
- `rating` — оценка 1–5
- `comment` — текст отзыва
- `review_time` — дата и время отзыва
- `metadata` — объект с `likes` и `dislikes`

**Пример документа:**
```json
{
  "product_id": 12,
  "user_id": 5,
  "rating": 5,
  "comment": "Отличный товар!",
  "review_time": "2024-03-10T10:12:00",
  "metadata": {
    "likes": 10,
    "dislikes": 1
  }
}
```

---

## 2. Таблицы в postgres (схема ods)

### users
Основная таблица пользователей. Поля: `id`, `name`, `email`, `age`

### users_address
Адреса пользователей (из поля `address`). Поля: `id`, `parent_id`, `street`, `city`, `zipcode`

### products
Основная таблица товаров. Поля: `id`, `name`, `category`, `price`

### products_tags
Теги товаров (из массива `tags`). Поля: `id`, `parent_id`, `value`

### orders
Основная таблица заказов. Поля: `id`, `user_id`, `total`, `status`, `order_time`

### orders_product_ids
Товары в заказах (из массива `product_ids`). Поля: `id`, `parent_id`, `product_id`

### reviews
Основная таблица отзывов. Поля: `id`, `product_id`, `user_id`, `rating`, `comment`, `review_time`

### reviews_metadata
Метаданные отзывов (из объекта `metadata`). Поля: `id`, `parent_id`, `likes`, `dislikes`

## Связи PostgreSQL

- `users.id → users_address.parent_id`  
- `users.id → orders.user_id`  
- `users.id → reviews.user_id`  
- `products.id → products_tags.parent_id`  
- `products.id → orders_product_ids.product_ids`  
- `products.id → reviews.product_id`  
- `orders.id → orders_product_ids.order_id`  
- `reviews.id → reviews_metadata.parent_id`

---

## 3. Витрины данных (схема marts)

## 3.1. sales_by_day
Витрина агрегирует продажи по дням.

**Источник:** ods.orders  

**Поля:**
- `order_date` — дата заказа (`DATE(order_time)`)  
- `orders_count` — количество заказов за день  
- `revenue` — суммарная выручка за день (`SUM(total)`)  
- `avg_check` — средний чек за день (`AVG(total)`)

**Назначение:**  
Позволяет анализировать динамику продаж по дням, строить графики выручки и оценивать средний чек.

**Пример строки:**
<table>
  <thead>
    <tr>
      <th>order_date</th>
      <th>orders_count</th>
      <th>revenue</th>
      <th>avg_check</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>2026-03-08</td>
      <td>123</td>
      <td>45678.90</td>
      <td>371.56</td>
    </tr>
  </tbody>
</table>

### 3.2. product_stats
Витрина агрегирует статистику по каждому товару.

**Источник:** ods.products, ods.orders_product_ids, ods.reviews  

**Поля:**
- `product_id` — уникальный идентификатор товара (`products.pg_id`)  
- `name` — название товара  
- `category` — категория товара  
- `sales_count` — количество продаж товара (по всем заказам)  
- `avg_rating` — средний рейтинг товара (по отзывам)  
- `reviews_count` — количество отзывов

**Назначение:**  
Позволяет оценивать популярность товаров, средний рейтинг, строить топ-продукты по продажам и отзывам.

**Пример строки:**

<table>
  <thead>
    <tr>
      <th>product_id</th>
      <th>name</th>
      <th>category</th>
      <th>sales_count</th>
      <th>avg_rating</th>
      <th>reviews_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>12</td>
      <td>Laptop</td>
      <td>Electronics</td>
      <td>45</td>
      <td>4.2</td>
      <td>12</td>
    </tr>
  </tbody>
</table>

---

## 4. Запуск

1. Склонируйте репозиторий
2. Дайте право исполнения файлу mongo-init ( ```bash chmod +x ./mongo-init/init.sh``` ) 
3. Создайте .env (```bash echo -e "AIRFLOW_UID=5000" > .env```)
4. Выполните ```bash docker compose up -d```
5. Перейдите по адресу http://localhost:8080/ Логин и пароль - airflow
6. Запустите даг migration_mongo_psql
7. Запустите даг build_data_marts
8. Результат работы дага можно посмотреть через DBeaver(порт 5432, логин и пароль - postgres)



















