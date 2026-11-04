#!/usr/bin/env python3
"""
Multi-DML transaction change generator for the Aurora PostgreSQL source
(PG -> Aurora DSQL migration / CDC lag test).

Unlike pg_tps.py (1 DML per autocommitted transaction), this generator groups
a RANDOM number of DML statements -- between --min-dml and --max-dml -- into a
single explicit BEGIN/COMMIT transaction. --rate controls TRANSACTIONS per
second, so DML/sec ~= rate * avg(min,max).

FK-safety (same guarantees as pg_tps.py):
  - INSERTs always reference parent ids in the seeded range [1, SEED_ROWS],
    which are never deleted -> inserts can't violate FKs.
  - DELETEs only target leaf tables (order_items, payments, shipments).
  - New rows use ids auto-detected above the seeded range at startup.
  - Each op in a transaction is independently FK-safe, so grouping is safe.

Usage:
  PGPASSWORD=... python3 pg_batch_load.py \
      --host <your-aurora-pg-endpoint> \
      --dbname postgres --user postgres \
      --rate 60 --min-dml 5 --max-dml 15 --duration 3600
"""

import argparse
import os
import random
import signal
import time

import psycopg2

SEED_ROWS   = 1_000_000
STATUS_ORDER = ['PENDING', 'PAID', 'SHIPPED', 'DELIVERED', 'CANCELLED']
PAY_METHOD   = ['CARD', 'PAYPAL', 'BANK_TRANSFER', 'COD']
PAY_STATUS   = ['AUTHORIZED', 'CAPTURED', 'REFUNDED', 'FAILED']
SHIP_STATUS  = ['PREPARING', 'IN_TRANSIT', 'DELIVERED', 'RETURNED']
CARRIERS     = ['UPS', 'FedEx', 'DHL', 'USPS']
CITIES       = ['Seattle', 'Austin', 'Portland', 'London', 'Berlin', 'Dubai', 'Seoul', 'Tokyo']
WEIGHTS      = [('insert', 45), ('update', 35), ('delete', 20)]

_stop = False


def _handle_stop(signum, frame):
    global _stop
    _stop = True
    print("\n[signal] stopping after current transaction...", flush=True)


def next_id(cur, table, pk):
    cur.execute(f"SELECT COALESCE(MAX({pk}), 0) FROM {table}")
    return max(cur.fetchone()[0], SEED_ROWS) + 1


def rnd_seed():
    return random.randint(1, SEED_ROWS)


def do_insert(cur, ids):
    c = random.random()
    if c < 0.40:
        oid = ids['orders']; ids['orders'] += 1
        cur.execute("INSERT INTO orders(order_id, customer_id, order_date, status, total_amount, shipping_address)"
                    " VALUES (%s,%s, now(), %s, %s, %s)",
                    (oid, rnd_seed(), random.choice(STATUS_ORDER), round(random.uniform(10, 2000), 2), f"Live addr {oid}"))
    elif c < 0.62:
        iid = ids['order_items']; ids['order_items'] += 1
        cur.execute("INSERT INTO order_items(order_item_id, order_id, product_id, quantity, unit_price)"
                    " VALUES (%s,%s,%s,%s,%s)",
                    (iid, rnd_seed(), rnd_seed(), random.randint(1, 10), round(random.uniform(5, 500), 2)))
    elif c < 0.80:
        pid = ids['payments']; ids['payments'] += 1
        cur.execute("INSERT INTO payments(payment_id, order_id, payment_date, amount, payment_method, status)"
                    " VALUES (%s,%s, now(), %s, %s, %s)",
                    (pid, rnd_seed(), round(random.uniform(10, 2000), 2), random.choice(PAY_METHOD), random.choice(PAY_STATUS)))
    elif c < 0.92:
        sid = ids['shipments']; ids['shipments'] += 1
        cur.execute("INSERT INTO shipments(shipment_id, order_id, carrier, tracking_number, shipped_date, status)"
                    " VALUES (%s,%s,%s,%s, now(), %s)",
                    (sid, rnd_seed(), random.choice(CARRIERS), f"TRK{sid:014d}", random.choice(SHIP_STATUS)))
    elif c < 0.97:
        cid = ids['customers']; ids['customers'] += 1
        cur.execute("INSERT INTO customers(customer_id, full_name, email, phone, city, country)"
                    " VALUES (%s,%s,%s,%s,%s,%s)",
                    (cid, f"Customer {cid}", f"customer{cid}@example.com", "+1-555-0000000", random.choice(CITIES), 'USA'))
    else:
        prid = ids['products']; ids['products'] += 1
        cur.execute("INSERT INTO products(product_id, sku, name, category, unit_price, stock_quantity)"
                    " VALUES (%s,%s,%s,%s,%s,%s)",
                    (prid, f"SKU-LIVE-{prid:010d}", f"Product {prid}", 'Electronics', round(random.uniform(5, 500), 2), random.randint(0, 1000)))


def do_update(cur, ids):
    c = random.random()
    if c < 0.30:
        cur.execute("UPDATE orders SET status=%s, total_amount=%s WHERE order_id=%s",
                    (random.choice(STATUS_ORDER), round(random.uniform(10, 2000), 2), rnd_seed()))
    elif c < 0.55:
        cur.execute("UPDATE products SET stock_quantity=%s, unit_price=%s WHERE product_id=%s",
                    (random.randint(0, 1000), round(random.uniform(5, 500), 2), rnd_seed()))
    elif c < 0.72:
        cur.execute("UPDATE payments SET status=%s WHERE payment_id=%s", (random.choice(PAY_STATUS), rnd_seed()))
    elif c < 0.88:
        cur.execute("UPDATE shipments SET status=%s, delivery_date=now() WHERE shipment_id=%s",
                    (random.choice(SHIP_STATUS), rnd_seed()))
    elif c < 0.96:
        cur.execute("UPDATE order_items SET quantity=%s WHERE order_item_id=%s", (random.randint(1, 10), rnd_seed()))
    else:
        cur.execute("UPDATE customers SET city=%s WHERE customer_id=%s", (random.choice(CITIES), rnd_seed()))


def do_delete(cur, ids):
    table, pk = random.choice([('order_items', 'order_item_id'), ('payments', 'payment_id'), ('shipments', 'shipment_id')])
    cur.execute(f"DELETE FROM {table} WHERE {pk}=%s", (random.randint(1, SEED_ROWS),))


def pick_op():
    r = random.randint(1, sum(w for _, w in WEIGHTS))
    upto = 0
    for name, w in WEIGHTS:
        upto += w
        if r <= upto:
            return name
    return WEIGHTS[-1][0]


def main():
    ap = argparse.ArgumentParser(description="Multi-DML (5-15 per txn) change generator for Aurora PG source.")
    ap.add_argument('--host', default=os.environ.get('PGHOST', 'localhost'))
    ap.add_argument('--port', default=int(os.environ.get('PGPORT', '5432')), type=int)
    ap.add_argument('--dbname', default=os.environ.get('PGDATABASE', 'postgres'))
    ap.add_argument('--user', default=os.environ.get('PGUSER', 'postgres'))
    ap.add_argument('--rate', type=float, default=60.0, help='target TRANSACTIONS per second')
    ap.add_argument('--min-dml', type=int, default=5, help='min DML statements per transaction')
    ap.add_argument('--max-dml', type=int, default=15, help='max DML statements per transaction')
    ap.add_argument('--duration', type=int, default=0, help='seconds to run; 0 = until signal')
    args = ap.parse_args()

    if args.min_dml < 1 or args.max_dml < args.min_dml:
        ap.error("require 1 <= --min-dml <= --max-dml")

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    conn = psycopg2.connect(host=args.host, port=args.port, dbname=args.dbname,
                            user=args.user, connect_timeout=10)
    conn.autocommit = False          # explicit multi-statement transactions
    cur = conn.cursor()

    ids = {
        'customers':   next_id(cur, 'customers', 'customer_id'),
        'products':    next_id(cur, 'products', 'product_id'),
        'orders':      next_id(cur, 'orders', 'order_id'),
        'order_items': next_id(cur, 'order_items', 'order_item_id'),
        'payments':    next_id(cur, 'payments', 'payment_id'),
        'shipments':   next_id(cur, 'shipments', 'shipment_id'),
    }
    conn.commit()

    interval = 1.0 / args.rate
    txns = 0
    dml_total = 0
    counts = {'insert': 0, 'update': 0, 'delete': 0}
    errors = 0
    start = time.perf_counter()
    deadline = start + args.duration if args.duration > 0 else None
    next_tick = start
    last_report = start

    print(f"[start] target {args.rate} TPS, {args.min_dml}-{args.max_dml} DML/txn "
          f"(~{args.rate * (args.min_dml + args.max_dml) / 2:.0f} DML/s) against "
          f"{args.host}:{args.port}/{args.dbname} "
          f"({'until stopped' if deadline is None else str(args.duration)+'s'}).", flush=True)

    while not _stop:
        now = time.perf_counter()
        if deadline and now >= deadline:
            break

        n = random.randint(args.min_dml, args.max_dml)
        try:
            for _ in range(n):
                op = pick_op()
                if op == 'insert':
                    do_insert(cur, ids)
                elif op == 'update':
                    do_update(cur, ids)
                else:
                    do_delete(cur, ids)
                counts[op] += 1
            conn.commit()
            txns += 1
            dml_total += n
        except Exception as e:
            errors += 1
            try:
                conn.rollback()
            except Exception:
                pass
            if errors <= 5:
                print(f"[warn] txn failed: {e}", flush=True)

        next_tick += interval
        sleep_for = next_tick - time.perf_counter()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_tick = time.perf_counter()

        if time.perf_counter() - last_report >= 5:
            elapsed = time.perf_counter() - start
            print(f"[{elapsed:6.1f}s] txns={txns:<6} dml={dml_total:<7} "
                  f"ins={counts['insert']} upd={counts['update']} del={counts['delete']} err={errors} | "
                  f"actual={txns/elapsed:4.1f} TPS, {dml_total/elapsed:5.1f} DML/s, "
                  f"avg_dml/txn={dml_total/max(txns,1):.1f}", flush=True)
            last_report = time.perf_counter()

    elapsed = time.perf_counter() - start
    print(f"\n[done] ran {elapsed:.1f}s | {txns} txns | {dml_total} DML | "
          f"ins={counts['insert']} upd={counts['update']} del={counts['delete']} err={errors} | "
          f"avg={txns/elapsed:.1f} TPS, {dml_total/elapsed:.1f} DML/s, "
          f"avg_dml/txn={dml_total/max(txns,1):.1f}", flush=True)
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
