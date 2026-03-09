#!/bin/bash
echo "Start mongo import"
for file in /docker-entrypoint-initdb.d/data/*.json
do
    collection=$(basename "$file" .json)
    echo "Importig $file to collection $collection"
    mongoimport \
        --db market \
        --collection "$collection" \
        --file "$file" \
        --type json \
        -u mongo \
        -p mongo \
        --authenticationDatabase admin
done
echo "Import done"