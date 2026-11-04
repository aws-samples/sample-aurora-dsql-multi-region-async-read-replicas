#!/usr/bin/env python3
"""
Customers-only change generator for the Aurora PostgreSQL source
(PG -> Aurora DSQL migration / CDC test).

Produces a steady mix of INSERT / UPDATE / DELETE against ONLY the customers
table at a target DML rate. Each operation is its own autocommitted
transaction, so rate == transactions/sec == CDC change events/sec.

FK-safety:
  - orders.customer_id references customers(customer_id), so deleting a seeded
    customer could violate that FK. This generator therefore only UPDATES and
    DELETES customers that IT inserted (ids >= the auto-detected high base),
    which no order row references. Seeded customers are never touched by DELETE.

Usage:
  PGPASSWORD=... python3 pg_customers_load.py \
      --host <your-aurora-pg-endpoint> \
      --dbname postgres --user postgres \
      --rate 5 --duration 0        # 0 = run until SIGINT/SIGTERM
"""

import argparse
import os
import random
import signal
import time

import psycopg2

CITIES    = ['Seattle', 'Austin', 'Portland', 'London', 'Berlin', 'Dubai', 'Seoul', 'Tokyo', 'Paris', 'Sydney']
COUNTRIES = ['USA', 'Canada', 'UK', 'Germany', 'Japan', 'UAE', 'South Korea', 'France', 'Australia']
FIRST     = ['Ava', 'Liam', 'Sofia', 'Noah', 'Mia', 'Ethan', 'Zoe', 'Lucas', 'Emma', 'Omar', 'Nina', 'Raj']
LAST      = ['Thompson', 'Patel', 'Garcia', 'Kim', 'Muller', 'Rossi', 'Chen', 'Haddad', 'Novak', 'Silva']

# INSERT-heavy so the pool of own-rows grows and sustains updates/deletes.
WEIGHTS = [('insert', 55), ('update', 30), ('delete', 15)]

_stop = False


def _handle_stop(signum, frame):
    global _stop
    _stop = True
    print("\n[signal] stopping after current op...", flush=True)


def pick_op():
    r = random.randint(1, sum(w for _, w in WEIGHTS))
    upto = 0
    for name, w in WEIGHTS:
        upto += w
        if r <= upto:
            return name
    return WEIGHTS[-1][0]


def main():
    ap = argparse.ArgumentParser(description="Customers-only DML generator for Aurora PG source.")
    ap.add_argument('--host', default=os.environ.get('PGHOST', 'localhost'))
    ap.add_argument('--port', default=int(os.environ.get('PGPORT', '5432')), type=int)
    ap.add_argument('--dbname', default=os.environ.get('PGDATABASE', 'postgres'))
    ap.add_argument('--user', default=os.environ.get('PGUSER', 'postgres'))
    ap.add_argument('--rate', type=float, default=5.0, help='target DML ops per second')
    ap.add_argument('--duration', type=int, default=0, help='seconds to run; 0 = until signal')
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    conn = psycopg2.connect(host=args.host, port=args.port, dbname=args.dbname,
                            user=args.user, connect_timeout=10)
    conn.autocommit = True
    cur = conn.cursor()

    # Start our own id range safely above everything currently present.
    cur.execute("SELECT COALESCE(MAX(customer_id), 0) FROM customers")
    base = max(cur.fetchone()[0], 90_000_000) + 1
    next_id = base
    own_ids = []          # customers this generator inserted and hasn't deleted

    interval = 1.0 / args.rate
    counts = {'insert': 0, 'update': 0, 'delete': 0, 'error': 0}
    start = time.perf_counter()
    deadline = start + args.duration if args.duration > 0 else None
    next_tick = start
    last_report = start

    print(f"[start] customers load: target {args.rate} DML/s against "
          f"{args.host}:{args.port}/{args.dbname}; own-id base={base} "
          f"({'until stopped' if deadline is None else str(args.duration)+'s'}).", flush=True)

    while not _stop:
        now = time.perf_counter()
        if deadline and now >= deadline:
            break

        op = pick_op()
        # Can't update/delete if we have no own rows yet -> fall back to insert.
        if op in ('update', 'delete') and not own_ids:
            op = 'insert'

        try:
            if op == 'insert':
                cid = next_id; next_id += 1
                name = f"{random.choice(FIRST)} {random.choice(LAST)}"
                cur.execute(
                    "INSERT INTO customers(customer_id, full_name, email, phone, city, country)"
                    " VALUES (%s,%s,%s,%s,%s,%s)",
                    (cid, name, f"load.{cid}@example.com",
                     f"+1-555-{cid % 10000:04d}", random.choice(CITIES), random.choice(COUNTRIES)))
                own_ids.append(cid)
            elif op == 'update':
                cid = random.choice(own_ids)
                cur.execute("UPDATE customers SET city=%s, phone=%s WHERE customer_id=%s",
                            (random.choice(CITIES), f"+1-555-{random.randint(0,9999):04d}", cid))
            else:  # delete one of our own rows (FK-safe: no orders reference these)
                cid = own_ids.pop(random.randrange(len(own_ids)))
                cur.execute("DELETE FROM customers WHERE customer_id=%s", (cid,))
            counts[op] += 1
        except Exception as e:
            counts['error'] += 1
            try:
                conn.rollback()
            except Exception:
                pass
            if counts['error'] <= 5:
                print(f"[warn] {op} failed: {e}", flush=True)

        next_tick += interval
        sleep_for = next_tick - time.perf_counter()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_tick = time.perf_counter()

        if time.perf_counter() - last_report >= 5:
            elapsed = time.perf_counter() - start
            total = counts['insert'] + counts['update'] + counts['delete']
            print(f"[{elapsed:6.1f}s] total={total:<6} ins={counts['insert']} "
                  f"upd={counts['update']} del={counts['delete']} err={counts['error']} "
                  f"live_own={len(own_ids)} | actual={total/elapsed:4.1f}/s", flush=True)
            last_report = time.perf_counter()

    elapsed = time.perf_counter() - start
    total = counts['insert'] + counts['update'] + counts['delete']
    print(f"\n[done] ran {elapsed:.1f}s | {total} ops | ins={counts['insert']} "
          f"upd={counts['update']} del={counts['delete']} err={counts['error']} | "
          f"avg={total/elapsed:.1f}/s | live_own_remaining={len(own_ids)}", flush=True)
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
