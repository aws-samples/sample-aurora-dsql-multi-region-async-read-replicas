#!/usr/bin/env python3
"""
Source-side change generator for Aurora PostgreSQL (PG -> Aurora DSQL migration test).

Produces a steady mix of INSERT / UPDATE / DELETE transactions at a target TPS
against the order-management schema (customers, products, orders, order_items,
payments, shipments). Each operation is its own autocommitted transaction, so
TPS == transactions/sec == CDC change events/sec on the source.

FK-safety:
  - INSERTs always reference parent ids in the seeded range [1, SEED_ROWS],
    which are never deleted, so inserts can't violate foreign keys.
  - DELETEs only target leaf tables (order_items, payments, shipments) which
    have no dependents, so deletes can't violate foreign keys.
  - New rows use ids starting above the seeded range (auto-detected at startup).

Usage:
  PGPASSWORD=... python3 tps_load.py \
      --host <your-aurora-pg-endpoint> \
      --dbname postgres --user postgres \
      --rate 40 --duration 300

  --duration 0  runs until Ctrl-C.
"""

import argparse
import os
import random
import signal
import sys
import time

import psycopg2

SEED_ROWS = 1_000_000          # rows pre-loaded per table (never deleted for parents)
STATUS_ORDER    = ['PENDING', 'PAID', 'SHIPPED', 'DELIVERED', 'CANCELLED']
PAY_METHOD      = ['CARD', 'PAYPAL', 'BANK_TRANSFER', 'COD']
PAY_STATUS      = ['AUTHORIZED', 'CAPTURED', 'REFUNDED', 'FAILED']
SHIP_STATUS     = ['PREPARING', 'IN_TRANSIT', 'DELIVERED', 'RETURNED']
CARRIERS        = ['UPS', 'FedEx', 'DHL', 'USPS']
CITIES          = ['Seattle', 'Austin', 'Portland', 'London', 'Berlin', 'Dubai', 'Seoul', 'Tokyo']

# Operation weights (within the full op mix). Normalized at runtime.
#   ~45% inserts, ~35% updates, ~20% deletes
WEIGHTS = [
    ('insert', 45),
    ('update', 35),
    ('delete', 20),
]

_stop = False


def _handle_sigint(signum, frame):
    global _stop
    _stop = True
    print("\n[signal] stopping after current window...", flush=True)


def next_id(cur, table, pk):
    cur.execute(f"SELECT COALESCE(MAX({pk}), 0) FROM {table}")
    return max(cur.fetchone()[0], SEED_ROWS) + 1


def rnd_seed():
    """A parent id guaranteed to still exist (seeded parents are never deleted)."""
    return random.randint(1, SEED_ROWS)


def do_insert(cur, ids):
    choice = random.random()
    if choice < 0.40:                      # order + its typical children (most common)
        oid = ids['orders']; ids['orders'] += 1
        cur.execute(
            "INSERT INTO orders(order_id, customer_id, order_date, status, total_amount, shipping_address)"
            " VALUES (%s,%s, now(), %s, %s, %s)",
            (oid, rnd_seed(), random.choice(STATUS_ORDER),
             round(random.uniform(10, 2000), 2), f"Live addr {oid}"))
    elif choice < 0.62:
        iid = ids['order_items']; ids['order_items'] += 1
        cur.execute(
            "INSERT INTO order_items(order_item_id, order_id, product_id, quantity, unit_price)"
            " VALUES (%s,%s,%s,%s,%s)",
            (iid, rnd_seed(), rnd_seed(), random.randint(1, 10),
             round(random.uniform(5, 500), 2)))
    elif choice < 0.80:
        pid = ids['payments']; ids['payments'] += 1
        cur.execute(
            "INSERT INTO payments(payment_id, order_id, payment_date, amount, payment_method, status)"
            " VALUES (%s,%s, now(), %s, %s, %s)",
            (pid, rnd_seed(), round(random.uniform(10, 2000), 2),
             random.choice(PAY_METHOD), random.choice(PAY_STATUS)))
    elif choice < 0.92:
        sid = ids['shipments']; ids['shipments'] += 1
        cur.execute(
            "INSERT INTO shipments(shipment_id, order_id, carrier, tracking_number, shipped_date, status)"
            " VALUES (%s,%s,%s,%s, now(), %s)",
            (sid, rnd_seed(), random.choice(CARRIERS),
             f"TRK{sid:014d}", random.choice(SHIP_STATUS)))
    elif choice < 0.97:
        cid = ids['customers']; ids['customers'] += 1
        cur.execute(
            "INSERT INTO customers(customer_id, full_name, email, phone, city, country)"
            " VALUES (%s,%s,%s,%s,%s,%s)",
            (cid, f"Customer {cid}", f"customer{cid}@example.com",
             "+1-555-0000000", random.choice(CITIES), 'USA'))
    else:
        prid = ids['products']; ids['products'] += 1
        cur.execute(
            "INSERT INTO products(product_id, sku, name, category, unit_price, stock_quantity)"
            " VALUES (%s,%s,%s,%s,%s,%s)",
            (prid, f"SKU-LIVE-{prid:010d}", f"Product {prid}",
             'Electronics', round(random.uniform(5, 500), 2), random.randint(0, 1000)))


def do_update(cur, ids):
    choice = random.random()
    if choice < 0.30:
        cur.execute("UPDATE orders SET status=%s, total_amount=%s WHERE order_id=%s",
                    (random.choice(STATUS_ORDER), round(random.uniform(10, 2000), 2), rnd_seed()))
    elif choice < 0.55:
        cur.execute("UPDATE products SET stock_quantity=%s, unit_price=%s WHERE product_id=%s",
                    (random.randint(0, 1000), round(random.uniform(5, 500), 2), rnd_seed()))
    elif choice < 0.72:
        cur.execute("UPDATE payments SET status=%s WHERE payment_id=%s",
                    (random.choice(PAY_STATUS), rnd_seed()))
    elif choice < 0.88:
        cur.execute("UPDATE shipments SET status=%s, delivery_date=now() WHERE shipment_id=%s",
                    (random.choice(SHIP_STATUS), rnd_seed()))
    elif choice < 0.96:
        cur.execute("UPDATE order_items SET quantity=%s WHERE order_item_id=%s",
                    (random.randint(1, 10), rnd_seed()))
    else:
        cur.execute("UPDATE customers SET city=%s WHERE customer_id=%s",
                    (random.choice(CITIES), rnd_seed()))


def do_delete(cur, ids):
    # Leaf tables only -> FK-safe. Random id may already be gone (affects 0 rows); still a txn.
    table, pk = random.choice([
        ('order_items', 'order_item_id'),
        ('payments', 'payment_id'),
        ('shipments', 'shipment_id'),
    ])
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
    ap = argparse.ArgumentParser(description="40 TPS change generator for Aurora PG source.")
    ap.add_argument('--host', default=os.environ.get('PGHOST', 'localhost'))
    ap.add_argument('--port', default=int(os.environ.get('PGPORT', '5432')), type=int)
    ap.add_argument('--dbname', default=os.environ.get('PGDATABASE', 'postgres'))
    ap.add_argument('--user', default=os.environ.get('PGUSER', 'postgres'))
    ap.add_argument('--rate', type=float, default=40.0, help='target transactions per second')
    ap.add_argument('--duration', type=int, default=0, help='seconds to run; 0 = until Ctrl-C')
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)

    conn = psycopg2.connect(host=args.host, port=args.port,
                            dbname=args.dbname, user=args.user,
                            connect_timeout=10)
    conn.autocommit = True   # one transaction per statement
    cur = conn.cursor()

    ids = {
        'customers':   next_id(cur, 'customers', 'customer_id'),
        'products':    next_id(cur, 'products', 'product_id'),
        'orders':      next_id(cur, 'orders', 'order_id'),
        'order_items': next_id(cur, 'order_items', 'order_item_id'),
        'payments':    next_id(cur, 'payments', 'payment_id'),
        'shipments':   next_id(cur, 'shipments', 'shipment_id'),
    }

    interval = 1.0 / args.rate
    counts = {'insert': 0, 'update': 0, 'delete': 0, 'error': 0}
    start = time.perf_counter()
    deadline = start + args.duration if args.duration > 0 else None
    next_tick = start
    last_report = start

    print(f"[start] target {args.rate} TPS against {args.host}:{args.port}/{args.dbname} "
          f"({'∞' if deadline is None else str(args.duration)+'s'}). Ctrl-C to stop.", flush=True)

    while not _stop:
        now = time.perf_counter()
        if deadline and now >= deadline:
            break

        op = pick_op()
        try:
            if op == 'insert':
                do_insert(cur, ids)
            elif op == 'update':
                do_update(cur, ids)
            else:
                do_delete(cur, ids)
            counts[op] += 1
        except Exception as e:                       # keep the generator alive
            counts['error'] += 1
            try:
                conn.rollback()
            except Exception:
                pass
            if counts['error'] <= 5:
                print(f"[warn] {op} failed: {e}", flush=True)

        # pace to target rate
        next_tick += interval
        sleep_for = next_tick - time.perf_counter()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_tick = time.perf_counter()         # fell behind; reset cadence

        # periodic report every ~5s
        if time.perf_counter() - last_report >= 5:
            elapsed = time.perf_counter() - start
            total = counts['insert'] + counts['update'] + counts['delete']
            print(f"[{elapsed:6.1f}s] total={total:<7} "
                  f"ins={counts['insert']} upd={counts['update']} del={counts['delete']} "
                  f"err={counts['error']} | actual={total/elapsed:5.1f} TPS", flush=True)
            last_report = time.perf_counter()

    elapsed = time.perf_counter() - start
    total = counts['insert'] + counts['update'] + counts['delete']
    print(f"\n[done] ran {elapsed:.1f}s | {total} txns | "
          f"ins={counts['insert']} upd={counts['update']} del={counts['delete']} err={counts['error']} | "
          f"avg={total/elapsed:.1f} TPS", flush=True)
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
