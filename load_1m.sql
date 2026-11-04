-- Step 1: Load ~1,000,000 rows into each of the 6 order-management tables.
-- Server-side synthetic generation via generate_series. Referentially consistent.
\timing on
\set N 1000000

-- Clean slate so FK ranges line up deterministically.
TRUNCATE shipments, payments, order_items, orders, products, customers RESTART IDENTITY CASCADE;

-- 1. customers (parents)
INSERT INTO customers (customer_id, full_name, email, phone, city, country)
SELECT g,
       'Customer ' || g,
       'customer' || g || '@example.com',
       '+1-555-' || lpad((g % 10000000)::text, 7, '0'),
       (ARRAY['Seattle','Austin','Portland','London','Berlin','Dubai','Seoul','Tokyo'])[1 + (g % 8)],
       (ARRAY['USA','USA','USA','UK','Germany','UAE','South Korea','Japan'])[1 + (g % 8)]
FROM generate_series(1, :N) g;

-- 2. products (parents)
INSERT INTO products (product_id, sku, name, category, unit_price, stock_quantity)
SELECT g,
       'SKU-' || lpad(g::text, 10, '0'),
       'Product ' || g,
       (ARRAY['Electronics','Furniture','Accessories','Office','Home'])[1 + (g % 5)],
       round((5 + (g % 50000) / 100.0)::numeric, 2),
       (g % 1000)
FROM generate_series(1, :N) g;

-- 3. orders -> customers
INSERT INTO orders (order_id, customer_id, order_date, status, total_amount, shipping_address)
SELECT g,
       1 + (g % :N),
       now() - ((g % 365) || ' days')::interval,
       (ARRAY['PENDING','PAID','SHIPPED','DELIVERED','CANCELLED'])[1 + (g % 5)],
       round((10 + (g % 100000) / 100.0)::numeric, 2),
       'Shipping address ' || g
FROM generate_series(1, :N) g;

-- 4. order_items -> orders, products  (line_total is generated)
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price)
SELECT g,
       1 + (g % :N),
       1 + (g % :N),
       1 + (g % 10),
       round((5 + (g % 50000) / 100.0)::numeric, 2)
FROM generate_series(1, :N) g;

-- 5. payments -> orders
INSERT INTO payments (payment_id, order_id, payment_date, amount, payment_method, status)
SELECT g,
       1 + (g % :N),
       now() - ((g % 365) || ' days')::interval,
       round((10 + (g % 100000) / 100.0)::numeric, 2),
       (ARRAY['CARD','PAYPAL','BANK_TRANSFER','COD'])[1 + (g % 4)],
       (ARRAY['AUTHORIZED','CAPTURED','REFUNDED','FAILED'])[1 + (g % 4)]
FROM generate_series(1, :N) g;

-- 6. shipments -> orders
INSERT INTO shipments (shipment_id, order_id, carrier, tracking_number, shipped_date, delivery_date, status)
SELECT g,
       1 + (g % :N),
       (ARRAY['UPS','FedEx','DHL','USPS'])[1 + (g % 4)],
       'TRK' || lpad(g::text, 14, '0'),
       now() - ((g % 365) || ' days')::interval,
       CASE WHEN g % 3 = 0 THEN now() - ((g % 30) || ' days')::interval ELSE NULL END,
       (ARRAY['PREPARING','IN_TRANSIT','DELIVERED','RETURNED'])[1 + (g % 4)]
FROM generate_series(1, :N) g;

ANALYZE;

SELECT 'customers'   AS table_name, count(*) FROM customers
UNION ALL SELECT 'products',    count(*) FROM products
UNION ALL SELECT 'orders',      count(*) FROM orders
UNION ALL SELECT 'order_items', count(*) FROM order_items
UNION ALL SELECT 'payments',    count(*) FROM payments
UNION ALL SELECT 'shipments',   count(*) FROM shipments
ORDER BY table_name;
