#! /usr/bin/bash

# Task 1 - Import ‘catalog.json’ into mongodb server into a database named ‘catalog’ and a collection named ‘electronics’
mongoimport -u root -p MTg1ODcteWxob29u --authenticationDatabase admin --db catalog --collection electronics --file /home/project/catalog.json

# Task 2 - List out all the databases
show dbs;

# Task 3 - List out all the collections in the database catalog
show collections;

# Task 4 - Create an index on the field “type”
db.electronics.createIndex({'type': 1})

db.electronics.aggregate([
  { $group: {"_id": "type", "count": {$count: "$type" } } },
  { $match: {"type": "laptop"} },
])

db.electronics.aggregate([
    { $match: { "type": "smart phone", "screen size": { $eq: 6 } } },
    { $count: "count" }
])


# Task 8 - Export the fields _id, “type”, “model”, from the ‘electronics’ collection into a file named electronics.csv
mongoexport -u root -p MTg1ODcteWxob29u --authenticationDatabase admin --db catalog --collection electronics --out /home/project/electronics.csv

mongoexport -u root -p MTg1ODcteWxob29u --db catalog --collection electronics --type csv --fields _id,type,model --out /home/project/electronics.csv


