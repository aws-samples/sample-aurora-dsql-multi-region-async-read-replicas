-- Append 5,000,000 MORE rows to each of the 6 tables (no truncate).
-- New ids use a fresh high range to avoid any PK collision with existing rows.
-- FK-safe: new orders reference seeded parents (1..1,000,000, never deleted);
-- new order_items/payments/shipments reference the new orders (same id range).
\timing on
\set LO 10000001
\set HI 15000000
\set SEED 1000000

-- parents first
INSERT INTO customers (customer_id, full_name, email, phone, city, country)
SELECT g,
       'Customer ' || g,
       'customer' || g || '@example.com',
       '+1-555-' || lpad((g % 10000000)::text, 7, '0'),
       (ARRAY['Seattle','Austin','Portland','London','Berlin','Dubai','Seoul','Tokyo'])[1 + (g % 8)],
       (ARRAY['USA','USA','USA','UK','Germany','UAE','South Korea','Japan'])[1 + (g % 8)]
FROM generate_series(:LO, :HI) g;

INSERT INTO products (product_id, sku, name, category, unit_price, stock_quantity)
SELECT g,
       'SKU-' || lpad(g::text, 10, '0'),
       'Product ' || g,
       (ARRAY['Electronics','Furniture','Accessories','Office','Home'])[1 + (g % 5)],
       round((5 + (g % 50000) / 100.0)::numeric, 2),
       (g % 1000)
FROM generate_series(:LO, :HI) g;

-- orders -> seeded customers (always exist)
INSERT INTO orders (order_id, customer_id, order_date, status, total_amount, shipping_address)
SELECT g,
       1 + (g % :SEED),
       now() - ((g % 365) || ' days')::interval,
       (ARRAY['PENDING','PAID','SHIPPED','DELIVERED','CANCELLED'])[1 + (g % 5)],
       round((10 + (g % 100000) / 100.0)::numeric, 2),
       'Shipping address ' || g
FROM generate_series(:LO, :HI) g;

-- order_items -> new orders (id = g) + seeded products
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price)
SELECT g,
       g,
       1 + (g % :SEED),
       1 + (g % 10),
       round((5 + (g % 50000) / 100.0)::numeric, 2)
FROM generate_series(:LO, :HI) g;

-- payments -> new orders (id = g)
INSERT INTO payments (payment_id, order_id, payment_date, amount, payment_method, status)
SELECT g,
       g,
       now() - ((g % 365) || ' days')::interval,
       round((10 + (g % 100000) / 100.0)::numeric, 2),
       (ARRAY['CARD','PAYPAL','BANK_TRANSFER','COD'])[1 + (g % 4)],
       (ARRAY['AUTHORIZED','CAPTURED','REFUNDED','FAILED'])[1 + (g % 4)]
FROM generate_series(:LO, :HI) g;

-- shipments -> new orders (id = g)
INSERT INTO shipments (shipment_id, order_id, carrier, tracking_number, shipped_date, delivery_date, status)
SELECT g,
       g,
       (ARRAY['UPS','FedEx','DHL','USPS'])[1 + (g % 4)],
       'TRK' || lpad(g::text, 14, '0'),
       now() - ((g % 365) || ' days')::interval,
       CASE WHEN g % 3 = 0 THEN now() - ((g % 30) || ' days')::interval ELSE NULL END,
       (ARRAY['PREPARING','IN_TRANSIT','DELIVERED','RETURNED'])[1 + (g % 4)]
FROM generate_series(:LO, :HI) g;

ANALYZE;

SELECT 'customers'   AS table_name, count(*) FROM customers
UNION ALL SELECT 'products',    count(*) FROM products
UNION ALL SELECT 'orders',      count(*) FROM orders
UNION ALL SELECT 'order_items', count(*) FROM order_items
UNION ALL SELECT 'payments',    count(*) FROM payments
UNION ALL SELECT 'shipments',   count(*) FROM shipments
ORDER BY table_name;
