"""
Verifies the live Azure PostgreSQL schema and writes docs/schema_report.md.

Checks, against the database itself (pg_catalog, so it works for a read-only
user too):
  * which tables, columns and types exist in the `stream` schema
  * primary keys, foreign keys, unique constraints and indexes
  * that the expected keys are really there (PASS / FAIL)
  * data integrity: no orphan rows, no duplicate dimension rows
  * row counts

Usage:  python verify_schema.py
Needs the DATABASE_* settings in .env (or environment variables).
"""

import os
import re
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv

load_dotenv()

SCHEMA = "stream"

# What the design says should exist.
EXPECTED = {
    "dim_device":        {"pk": ["device_id"], "unique": ["device_code"], "fk": {}},
    "dim_location":      {"pk": ["location_id"], "unique": ["city"], "fk": {}},
    "fact_event_stream": {"pk": ["event_id"], "unique": [],
                          "fk": {"device_id": "dim_device", "location_id": "dim_location"}},
    "ml_predictions":    {"pk": ["event_id"], "unique": [],
                          "fk": {"event_id": "fact_event_stream"}},
    "ml_state":          {"pk": ["name"], "unique": [], "fk": {}},
}

COLUMNS_SQL = """
SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod),
       a.attnotnull, pg_get_expr(d.adbin, d.adrelid)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
WHERE n.nspname = %s AND c.relkind = 'r'
ORDER BY c.relname, a.attnum
"""

CONSTRAINTS_SQL = """
SELECT c.relname, con.conname, con.contype, pg_get_constraintdef(con.oid)
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %s
ORDER BY c.relname, con.contype, con.conname
"""

INDEXES_SQL = """
SELECT tablename, indexname, indexdef
FROM pg_indexes WHERE schemaname = %s ORDER BY tablename, indexname
"""

INTEGRITY_CHECKS = [
    ("Readings whose device_id is missing from dim_device",
     "SELECT COUNT(*) FROM stream.fact_event_stream f "
     "LEFT JOIN stream.dim_device d ON d.device_id = f.device_id "
     "WHERE d.device_id IS NULL"),
    ("Readings whose location_id is missing from dim_location",
     "SELECT COUNT(*) FROM stream.fact_event_stream f "
     "LEFT JOIN stream.dim_location l ON l.location_id = f.location_id "
     "WHERE l.location_id IS NULL"),
    ("Duplicate device_code values in dim_device",
     "SELECT COUNT(*) FROM (SELECT device_code FROM stream.dim_device "
     "GROUP BY device_code HAVING COUNT(*) > 1) t"),
    ("Duplicate city values in dim_location",
     "SELECT COUNT(*) FROM (SELECT city FROM stream.dim_location "
     "GROUP BY city HAVING COUNT(*) > 1) t"),
    ("Predictions whose device_id differs from their reading's device_id "
     "(ml_predictions.device_id is not a foreign key, so this is checked here)",
     "SELECT COUNT(*) FROM stream.ml_predictions p "
     "JOIN stream.fact_event_stream f ON f.event_id = p.event_id "
     "WHERE p.device_id <> f.device_id"),
]


def md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    for r in rows:
        out.append("| " + " | ".join("" if v is None else str(v).replace("|", "/") for v in r) + " |")
    return "\n".join(out)


def check_expected(constraints):
    """constraints: {table: [(name, type, definition)]} -> list of (ok, text)."""
    results = []
    for table, exp in EXPECTED.items():
        defs = constraints.get(table)
        if defs is None:
            results.append((False, f"{table}: table is missing or has no constraints"))
            continue
        texts = [d for _, _, d in defs]
        for col in exp["pk"]:
            ok = any(t == f"PRIMARY KEY ({col})" for t in texts)
            results.append((ok, f"{table}: PRIMARY KEY ({col})"))
        for col in exp["unique"]:
            ok = any(t == f"UNIQUE ({col})" for t in texts)
            results.append((ok, f"{table}: UNIQUE ({col})"))
        for col, ref in exp["fk"].items():
            pattern = rf"FOREIGN KEY \({col}\) REFERENCES (?:{SCHEMA}\.)?{ref}\("
            ok = any(re.search(pattern, t) for t in texts)
            results.append((ok, f"{table}: FOREIGN KEY ({col}) -> {ref}"))
    return results


def main():
    conn = psycopg2.connect(
        host=os.environ["DATABASE_HOST"], dbname=os.environ["DATABASE_NAME"],
        user=os.environ["DATABASE_USER"], password=os.environ["DATABASE_PASSWORD"],
        port=os.environ.get("DATABASE_PORT", "5432"), sslmode="require", connect_timeout=15,
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(COLUMNS_SQL, (SCHEMA,))
    columns = {}
    for table, col, typ, notnull, default in cur.fetchall():
        columns.setdefault(table, []).append((col, typ, "NOT NULL" if notnull else "", default))

    cur.execute(CONSTRAINTS_SQL, (SCHEMA,))
    kinds = {"p": "PRIMARY KEY", "f": "FOREIGN KEY", "u": "UNIQUE", "c": "CHECK"}
    constraints = {}
    for table, name, ctype, definition in cur.fetchall():
        constraints.setdefault(table, []).append((name, kinds.get(ctype, ctype), definition))

    cur.execute(INDEXES_SQL, (SCHEMA,))
    indexes = cur.fetchall()

    counts = {}
    for table in columns:
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{table}")
        counts[table] = cur.fetchone()[0]

    expectations = check_expected(constraints)

    integrity = []
    for label, sql in INTEGRITY_CHECKS:
        try:
            cur.execute(sql)
            integrity.append((label, cur.fetchone()[0]))
        except psycopg2.errors.UndefinedTable:
            integrity.append((label, "table not found"))
    conn.close()

    # ---- console summary ----
    print(f"Schema '{SCHEMA}': {len(columns)} tables")
    for table in columns:
        print(f"  {table:20s} {counts[table]:>10,} rows, {len(columns[table])} columns")
    print("\nExpected keys:")
    for ok, text in expectations:
        print(f"  [{'PASS' if ok else 'FAIL'}] {text}")
    print("\nIntegrity checks (0 is good):")
    for label, value in integrity:
        print(f"  {value}  <- {label}")

    # ---- markdown report ----
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"# Schema verification report", "",
             f"Generated {now} by `verify_schema.py` from the live Azure PostgreSQL database.", ""]
    lines += ["## Tables and row counts", "",
              md_table(["Table", "Rows", "Columns"],
                       [(t, f"{counts[t]:,}", len(columns[t])) for t in columns]), ""]
    for table in columns:
        lines += [f"## {SCHEMA}.{table}", "",
                  md_table(["Column", "Type", "Null?", "Default"], columns[table]), ""]
        if table in constraints:
            lines += ["Constraints:", "",
                      md_table(["Name", "Kind", "Definition"], constraints[table]), ""]
    lines += ["## Indexes", "", md_table(["Table", "Index", "Definition"], indexes), ""]
    lines += ["## Expected keys", "",
              md_table(["Result", "Check"], [("PASS" if ok else "FAIL", t) for ok, t in expectations]), ""]
    lines += ["## Integrity checks (0 is good)", "",
              md_table(["Check", "Result"], integrity), ""]

    out_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "schema_report.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"\nFull report written to {path}")


if __name__ == "__main__":
    main()