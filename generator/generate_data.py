import os
import random
from datetime import datetime, timedelta

from dotenv import load_dotenv
from faker import Faker
from psycopg import connect


load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"), override=False)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    return int(raw)


def main() -> int:
    fake = Faker("ru_RU")

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    public_port = int(os.getenv("POSTGRES_PUBLIC_PORT", str(port)))
    db = os.getenv("POSTGRES_DB", "shop")
    user = os.getenv("POSTGRES_USER", "admin")
    password = os.getenv("POSTGRES_PASSWORD", "secret")

    customers_n = _env_int("GEN_CUSTOMERS", 100_000)
    orders_n = _env_int("GEN_ORDERS", 500_000)
    batch = _env_int("GEN_BATCH", 5_000)

    products = [
        "Ноутбук",
        "Мышь",
        "Монитор",
        "Клавиатура",
        "Наушники",
        "SSD",
        "Видеокарта",
        "Коврик для мыши",
        "Смартфон",
        "Планшет",
    ]
    statuses = ["pending", "completed", "shipped", "cancelled", "refunded"]

    # When running locally (Windows host), docker service names like "postgres"
    # won't resolve. In that case use the published port on localhost.
    if host in {"postgres", "master_db"}:
        host = "localhost"
        port = public_port

    print(
        f"Connecting to postgres {host}:{port}/{db} as {user}. "
        f"Will generate customers={customers_n}, orders={orders_n} (batch={batch})."
    )

    with connect(host=host, port=port, dbname=db, user=user, password=password) as conn:
        with conn.cursor() as cur:
            # Customers
            created_base = datetime.utcnow() - timedelta(days=60)
            print("Generating customers...")
            for i in range(0, customers_n, batch):
                rows = []
                for _ in range(min(batch, customers_n - i)):
                    name = fake.name()
                    email = fake.unique.email()
                    created_at = created_base + timedelta(seconds=random.randint(0, 60 * 24 * 3600))
                    rows.append((name, email, created_at))
                cur.executemany(
                    "INSERT INTO customers (name, email, created_at) VALUES (%s, %s, %s)",
                    rows,
                )
                conn.commit()
                print(f"  customers: {min(i + batch, customers_n)}/{customers_n}")

            cur.execute("SELECT max(id) FROM customers")
            max_customer_id = cur.fetchone()[0]
            if not max_customer_id:
                raise RuntimeError("No customers generated")

            # Orders
            print("Generating orders...")
            for i in range(0, orders_n, batch):
                rows = []
                for _ in range(min(batch, orders_n - i)):
                    customer_id = random.randint(1, int(max_customer_id))
                    product = random.choice(products)
                    amount = round(random.uniform(200, 150_000), 2)
                    status = random.choice(statuses)
                    created_at = created_base + timedelta(seconds=random.randint(0, 60 * 24 * 3600))
                    updated_at = created_at + timedelta(seconds=random.randint(0, 7 * 24 * 3600))
                    rows.append((customer_id, product, amount, status, created_at, updated_at))
                cur.executemany(
                    "INSERT INTO orders (customer_id, product, amount, status, created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    rows,
                )
                conn.commit()
                print(f"  orders: {min(i + batch, orders_n)}/{orders_n}")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

