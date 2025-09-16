CREATE DATABASE IF NOT EXISTS fuhsin_erp_demo CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE fuhsin_erp_demo;

DROP TABLE IF EXISTS product_master_a;
CREATE TABLE product_master_a (
  product_id        VARCHAR(32) PRIMARY KEY,
  product_name      VARCHAR(255),
  product_model     VARCHAR(255),
  category          VARCHAR(255),
  price             DECIMAL(12,2),
  stock_qty         INT,
  manufacture_date  DATE,
  supplier          VARCHAR(255),
  status            VARCHAR(32),
  last_modified     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS product_warehouse_b;
CREATE TABLE product_warehouse_b (
  product_id          VARCHAR(32),
  product_name        VARCHAR(255),
  warehouse_location  VARCHAR(255),
  quantity            INT,
  last_inventory_date DATE,
  manager             VARCHAR(255),
  special_notes       VARCHAR(500),
  min_stock_level     INT,
  last_modified       TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY (product_id)
);

DROP TABLE IF EXISTS customer_complaint_c;
CREATE TABLE customer_complaint_c (
  complaint_id     VARCHAR(32) PRIMARY KEY,
  complaint_date   DATE,
  customer_name    VARCHAR(255),
  customer_company VARCHAR(255),
  complaint_type   VARCHAR(255),
  severity         VARCHAR(32),
  description      TEXT,
  handler          VARCHAR(255),
  status           VARCHAR(32),
  resolution_date  DATE,
  last_modified    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
