import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection


load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"), override=False)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _log(level: str, msg: str) -> None:
    configured = os.getenv("LOG_LEVEL", "INFO").upper()
    order = {"DEBUG": 10, "INFO": 20, "WARN": 30, "WARNING": 30, "ERROR": 40}
    if order.get(level, 20) >= order.get(configured, 20):
        ts = _utcnow().isoformat(timespec="seconds")
        print(f"{ts} [{level}] {msg}", flush=True)


@dataclass(frozen=True)
class Settings:
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str
    mongo_host: str
    mongo_port: int
    mongo_db: str
    state_doc_id: str
    sync_window_lag_seconds: int


def load_settings() -> Settings:
    missing = [
        k
        for k in [
            "POSTGRES_HOST",
            "POSTGRES_PORT",
            "POSTGRES_DB",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "MONGO_HOST",
            "MONGO_PORT",
            "MONGO_DB",
        ]
        if not os.getenv(k)
    ]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

    return Settings(
        pg_host=os.environ["POSTGRES_HOST"],
        pg_port=int(os.environ["POSTGRES_PORT"]),
        pg_db=os.environ["POSTGRES_DB"],
        pg_user=os.environ["POSTGRES_USER"],
        pg_password=os.environ["POSTGRES_PASSWORD"],
        mongo_host=os.environ["MONGO_HOST"],
        mongo_port=int(os.environ["MONGO_PORT"]),
        mongo_db=os.environ["MONGO_DB"],
        state_doc_id=os.getenv("STATE_DOC_ID", "replication_state"),
        sync_window_lag_seconds=_env_int("SYNC_WINDOW_LAG_SECONDS", 5),
    )


def pg_connect(s: Settings):
    return psycopg2.connect(
        host=s.pg_host,
        port=s.pg_port,
        dbname=s.pg_db,
        user=s.pg_user,
        password=s.pg_password,
        cursor_factory=RealDictCursor,
    )


def mongo_connect(s: Settings) -> MongoClient:
    return MongoClient(host=s.mongo_host, port=s.mongo_port, serverSelectionTimeoutMS=5000)


def _coerce_decimal(v: Any) -> Any:
    if isinstance(v, Decimal):
        return float(v)
    return v


def read_last_sync_time(state: Collection, state_doc_id: str) -> datetime:
    doc = state.find_one({"_id": state_doc_id})
    if not doc or "last_sync" not in doc:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    last = doc["last_sync"]
    if isinstance(last, datetime):
        if last.tzinfo is None:
            return last.replace(tzinfo=timezone.utc)
        return last.astimezone(timezone.utc)
    return datetime(1970, 1, 1, tzinfo=timezone.utc)


def save_last_sync_time(state: Collection, state_doc_id: str, ts: datetime) -> None:
    state.update_one(
        {"_id": state_doc_id},
        {"$set": {"last_sync": ts, "updated_at": _utcnow()}},
        upsert=True,
    )


def extract_new_customers(pg_cur, last_sync: datetime) -> List[Dict[str, Any]]:
    pg_cur.execute(
        """
        SELECT id, name, email, created_at
        FROM customers
        WHERE created_at > %s
        ORDER BY created_at ASC
        """,
        (last_sync,),
    )
    return list(pg_cur.fetchall())


def extract_new_or_updated_orders(pg_cur, last_sync: datetime) -> List[Dict[str, Any]]:
    pg_cur.execute(
        """
        SELECT
            o.id              AS order_id,
            o.customer_id     AS customer_id,
            o.product         AS product,
            o.amount          AS amount,
            o.status          AS status,
            o.created_at      AS created_at,
            o.updated_at      AS updated_at,
            c.name            AS customer_name,
            c.email           AS customer_email,
            c.created_at      AS customer_created_at
        FROM orders o
        JOIN customers c ON c.id = o.customer_id
        WHERE o.updated_at > %s
        ORDER BY o.updated_at ASC
        """,
        (last_sync,),
    )
    return list(pg_cur.fetchall())


def upsert_customer_doc(customers: Collection, row: Dict[str, Any]) -> None:
    customer_id = int(row["id"])
    customers.update_one(
        {"_id": customer_id},
        {
            "$setOnInsert": {"orders": []},
            "$set": {
                "name": row["name"],
                "email": row["email"],
                "synced_at": _utcnow(),
            },
        },
        upsert=True,
    )


def ensure_customer_from_order(customers: Collection, order_row: Dict[str, Any]) -> None:
    customer_id = int(order_row["customer_id"])
    customers.update_one(
        {"_id": customer_id},
        {
            "$setOnInsert": {"orders": []},
            "$set": {
                "name": order_row["customer_name"],
                "email": order_row["customer_email"],
                "synced_at": _utcnow(),
            },
        },
        upsert=True,
    )


def transform_order(order_row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "order_id": int(order_row["order_id"]),
        "product": order_row["product"],
        "amount": _coerce_decimal(order_row["amount"]),
        "status": order_row["status"],
        "placed_at": (
            order_row["created_at"].replace(tzinfo=timezone.utc)
            if isinstance(order_row["created_at"], datetime) and order_row["created_at"].tzinfo is None
            else order_row["created_at"]
        ),
        "updated_at": (
            order_row["updated_at"].replace(tzinfo=timezone.utc)
            if isinstance(order_row["updated_at"], datetime) and order_row["updated_at"].tzinfo is None
            else order_row["updated_at"]
        ),
    }


def upsert_embedded_order(customers: Collection, customer_id: int, order_doc: Dict[str, Any]) -> None:
    order_id = order_doc["order_id"]

    res = customers.update_one(
        {"_id": customer_id, "orders.order_id": order_id},
        {"$set": {"orders.$": order_doc, "synced_at": _utcnow()}},
        upsert=False,
    )
    if res.matched_count == 0:
        customers.update_one(
            {"_id": customer_id},
            {"$push": {"orders": order_doc}, "$set": {"synced_at": _utcnow()}},
            upsert=True,
        )


def replicate_once() -> int:
    s = load_settings()

    mongo = mongo_connect(s)
    mongo.admin.command("ping")

    db = mongo[s.mongo_db]
    customers_col = db["customers"]
    state_col = db["etl_state"]

    last_sync = read_last_sync_time(state_col, s.state_doc_id)
    cutoff = _utcnow() - timedelta(seconds=s.sync_window_lag_seconds)

    _log("INFO", f"Last sync: {last_sync.isoformat()} | Cutoff: {cutoff.isoformat()}")

    replicated_customers = 0
    replicated_orders = 0

    with pg_connect(s) as pg_conn:
        with pg_conn.cursor() as cur:
            new_customers = extract_new_customers(cur, last_sync)
            for c in new_customers:
                upsert_customer_doc(customers_col, c)
            replicated_customers = len(new_customers)

            new_orders = extract_new_or_updated_orders(cur, last_sync)
            for o in new_orders:
                ensure_customer_from_order(customers_col, o)
                order_doc = transform_order(o)
                upsert_embedded_order(customers_col, int(o["customer_id"]), order_doc)
            replicated_orders = len(new_orders)

    # Save state at cutoff to reduce missing late-committed rows
    save_last_sync_time(state_col, s.state_doc_id, cutoff)

    _log("INFO", f"Replicated: {replicated_customers} customers, {replicated_orders} orders")
    return 0


def main() -> int:
    start = time.time()
    try:
        code = replicate_once()
        return code
    except Exception as e:
        _log("ERROR", f"Replication failed: {e!r}")
        return 2
    finally:
        elapsed = time.time() - start
        _log("DEBUG", f"Elapsed: {elapsed:.3f}s")


if __name__ == "__main__":
    raise SystemExit(main())

