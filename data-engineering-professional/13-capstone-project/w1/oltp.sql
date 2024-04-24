-- Task 1 - Create a database
CREATE DATABASE sales;

-- Task 2 - Design a table named sales_data
CREATE TABLE sales_data (
  `product_id` int NOT NULL,
  `customer_id` int NOT NULL,
  `price` int NOT NULL,
  `quantity` int NOT NULL,
  `timestamp` timestamp NOT NULL
);

-- Task 5. Write a query to find out the count of records in the tables sales_data
SELECT COUNT(*) FROM sales_data;

-- Task 6 - Create an index
CREATE INDEX timestamp_index ON sales_data(timestamp);

-- Task 7 - List indexes
SHOW INDEX FROM sales_data;

-- Task 8 - Write a bash script to export data
