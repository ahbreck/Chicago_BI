import os
import sys
import time
from psycopg2 import sql
import psycopg2
from flask import Flask, abort, render_template, request

app = Flask(__name__)

ALLOWED_TABLES = {
    "req_2_airport_trips": "Airport Trips by Zip",
    "req_3_ccvi_trips": "Trips by CCVI Category",
    "req_5_disadv_perm": "Disadvantaged Permits",
    "req_6_loan_elig_permits": "Loan Eligibility Permits",
}

DEFAULT_LIMIT = 100
MAX_LIMIT = 500
TABLE_CHECK_TIMEOUT = int(os.getenv("TABLE_CHECK_TIMEOUT", "240"))
TABLE_CHECK_INTERVAL = int(os.getenv("TABLE_CHECK_INTERVAL", "3"))
DEFAULT_STARTUP_DELAY_MINUTES = 4
REQUIRED_TABLES = list(ALLOWED_TABLES.keys())
_tables_ready = False


def get_database_url() -> str:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    return db_url


def fetch_table_rows(table_name: str, limit: int, offset: int):
    query = sql.SQL("SELECT * FROM {} LIMIT %s OFFSET %s").format(
        sql.Identifier(table_name)
    )
    count_query = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name))

    with psycopg2.connect(get_database_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (limit, offset))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

            cur.execute(count_query)
            total = cur.fetchone()[0] if cur.rowcount != 0 else 0

    return rows, columns, total


def sanitize_pagination() -> tuple[int, int]:
    try:
        limit = int(request.args.get("limit", DEFAULT_LIMIT))
    except ValueError:
        limit = DEFAULT_LIMIT

    try:
        offset = int(request.args.get("offset", 0))
    except ValueError:
        offset = 0

    limit = max(1, min(limit, MAX_LIMIT))
    offset = max(0, offset)
    return limit, offset


def _startup_delay_seconds() -> int:
    raw_value = os.getenv("STARTUP_DELAY_MINUTES", str(DEFAULT_STARTUP_DELAY_MINUTES)).strip()
    try:
        minutes = int(raw_value)
    except ValueError:
        print(
            f"Invalid STARTUP_DELAY_MINUTES={raw_value!r}; defaulting to {DEFAULT_STARTUP_DELAY_MINUTES} minutes",
            file=sys.stderr,
        )
        minutes = DEFAULT_STARTUP_DELAY_MINUTES

    if minutes < 0:
        print(
            f"STARTUP_DELAY_MINUTES is negative ({minutes}); defaulting to {DEFAULT_STARTUP_DELAY_MINUTES} minutes",
            file=sys.stderr,
        )
        minutes = DEFAULT_STARTUP_DELAY_MINUTES

    return minutes * 60


def missing_required_tables() -> list[str]:
    with psycopg2.connect(get_database_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = ANY(%s)
                """,
                (REQUIRED_TABLES,),
            )
            existing = {row[0] for row in cur.fetchall()}
            return [name for name in REQUIRED_TABLES if name not in existing]


def wait_for_required_tables(startup_delay_seconds: int = 0):
    """Block startup until required tables exist (bounded retry)."""
    global _tables_ready
    if startup_delay_seconds > 0:
        print(
            f"Waiting {startup_delay_seconds} seconds before checking for required tables...",
            file=sys.stderr,
        )
        time.sleep(startup_delay_seconds)

    deadline = time.time() + TABLE_CHECK_TIMEOUT
    last_error = None
    while time.time() < deadline:
        try:
            missing = missing_required_tables()
            if not missing:
                _tables_ready = True
                return
        except Exception as exc:  # pragma: no cover - runtime guard
            last_error = exc
        time.sleep(TABLE_CHECK_INTERVAL)

    missing_tables = f"Missing tables: {', '.join(missing)}" if "missing" in locals() and missing else "Missing tables: unknown"
    last_err_msg = f"; last error: {last_error}" if last_error else ""
    raise RuntimeError(
        f"Required tables not ready after {TABLE_CHECK_TIMEOUT}s. {missing_tables}{last_err_msg}"
    )


@app.route("/")
def index():
    return render_template("index.html", tables=ALLOWED_TABLES)


@app.route("/table/<table_key>")
def show_table(table_key: str):
    if table_key not in ALLOWED_TABLES:
        abort(404)

    limit, offset = sanitize_pagination()
    error_message = None
    rows = []
    columns = []
    total_rows = 0
    try:
        rows, columns, total_rows = fetch_table_rows(table_key, limit, offset)
    except psycopg2.errors.UndefinedTable:
        error_message = "Data not ready yet; the reports service is still building this table."
    except psycopg2.Error as exc:  # pragma: no cover - runtime DB failure
        error_message = f"Unable to fetch data: {exc}"

    next_offset = offset + limit if offset + limit < total_rows else None
    prev_offset = offset - limit if offset - limit >= 0 else None

    return render_template(
        "table.html",
        table_key=table_key,
        table_name=ALLOWED_TABLES[table_key],
        rows=rows,
        columns=columns,
        limit=limit,
        offset=offset,
        total_rows=total_rows,
        next_offset=next_offset,
        prev_offset=prev_offset,
        error=error_message,
    )


@app.route("/healthz")
def healthcheck():
    return {"status": "ok"}


@app.route("/readyz")
def readiness():
    return {"ready": _tables_ready}, (200 if _tables_ready else 503)


startup_delay_seconds = _startup_delay_seconds()
try:
    wait_for_required_tables(startup_delay_seconds=startup_delay_seconds)
except RuntimeError as exc:  # pragma: no cover - startup failure path
    print(str(exc), file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
