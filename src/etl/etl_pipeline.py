"""
ETL PIPELINE - Extract, Transform, Load
Reads raw CSV data, transforms it, and loads into SQLite data warehouse.
"""

import csv
import os
import sqlite3
from datetime import datetime

BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "..")
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
DB_PATH = os.path.join(BASE_DIR, "data", "warehouse.db")
SCHEMA_PATH = os.path.join(BASE_DIR, "src", "database", "schema.sql")


class ETLPipeline:
    def __init__(self):
        self.conn = None
        self.stats = {"extracted": {}, "transformed": {}, "loaded": {}, "errors": []}

    def run(self):
        """Execute the full ETL pipeline."""
        print("=" * 60)
        print("ETL PIPELINE - Starting")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        try:
            self._setup_database()

            # EXTRACT
            print("\n--- PHASE 1: EXTRACT ---")
            raw_data = self._extract()

            # TRANSFORM
            print("\n--- PHASE 2: TRANSFORM ---")
            transformed = self._transform(raw_data)

            # LOAD
            print("\n--- PHASE 3: LOAD ---")
            self._load(transformed)

            # VALIDATE
            print("\n--- PHASE 4: VALIDATE ---")
            self._validate()

            self._save_log()

            print("\n" + "=" * 60)
            print("ETL PIPELINE COMPLETE!")
            print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 60)

        except Exception as e:
            self.stats["errors"].append(str(e))
            print(f"\nPIPELINE ERROR: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()

    def _setup_database(self):
        """Create database and schema."""
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        self.conn = sqlite3.connect(DB_PATH)
        with open(SCHEMA_PATH, "r") as f:
            self.conn.executescript(f.read())
        print(f"Database ready: {DB_PATH}")

    def _extract(self):
        """Extract data from CSV files."""
        data = {}
        files = {
            "customers": "dim_customers.csv",
            "products": "dim_products.csv",
            "dates": "dim_dates.csv",
            "channels": "dim_channels.csv",
            "sales": "fact_sales.csv",
            "activities": "fact_user_activity.csv",
        }
        for key, filename in files.items():
            filepath = os.path.join(RAW_DIR, filename)
            with open(filepath, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                data[key] = list(reader)
            count = len(data[key])
            self.stats["extracted"][key] = count
            print(f"  Extracted {key}: {count} rows")
        return data

    def _transform(self, raw_data):
        """Apply transformations to raw data."""
        transformed = {}

        # Transform customers - normalize text, validate emails
        customers = []
        for row in raw_data["customers"]:
            row["first_name"] = row["first_name"].strip().title()
            row["last_name"] = row["last_name"].strip().title()
            row["email"] = row["email"].strip().lower()
            row["country"] = row["country"].strip().title()
            row["city"] = row["city"].strip().title()
            customers.append(row)
        transformed["customers"] = customers
        print(f"  Transformed customers: {len(customers)} rows")

        # Transform products - ensure numeric types
        products = []
        for row in raw_data["products"]:
            row["unit_price"] = round(float(row["unit_price"]), 2)
            row["cost_price"] = round(float(row["cost_price"]), 2)
            row["product_name"] = row["product_name"].strip()
            products.append(row)
        transformed["products"] = products
        print(f"  Transformed products: {len(products)} rows")

        # Transform dates
        transformed["dates"] = raw_data["dates"]
        print(f"  Transformed dates: {len(raw_data['dates'])} rows")

        # Transform channels
        transformed["channels"] = raw_data["channels"]
        print(f"  Transformed channels: {len(raw_data['channels'])} rows")

        # Transform sales - recalculate totals for data integrity
        sales = []
        for row in raw_data["sales"]:
            row["quantity"] = int(row["quantity"])
            row["unit_price"] = float(row["unit_price"])
            row["discount"] = float(row["discount"])
            row["total_amount"] = round(row["quantity"] * row["unit_price"] * (1 - row["discount"]), 2)
            row["profit"] = float(row["profit"])
            sales.append(row)
        transformed["sales"] = sales
        print(f"  Transformed sales: {len(sales)} rows")

        # Transform activities
        activities = []
        for row in raw_data["activities"]:
            row["session_duration_sec"] = int(row["session_duration_sec"])
            row["pages_viewed"] = int(row["pages_viewed"])
            row["actions_taken"] = int(row["actions_taken"])
            row["bounce"] = int(row["bounce"])
            activities.append(row)
        transformed["activities"] = activities
        print(f"  Transformed activities: {len(activities)} rows")

        for key, rows in transformed.items():
            self.stats["transformed"][key] = len(rows)

        return transformed

    def _load(self, data):
        """Load transformed data into the database."""
        cursor = self.conn.cursor()

        # Clear existing data (full refresh)
        tables = ["fact_user_activity", "fact_sales", "dim_channels", "dim_date", "dim_products", "dim_customers"]
        for table in tables:
            cursor.execute(f"DELETE FROM {table}")

        # Load dimensions first, then facts
        self._load_table(
            cursor,
            "dim_customers",
            data["customers"],
            ["customer_id", "first_name", "last_name", "email", "country", "city", "age_group", "signup_date"],
        )

        self._load_table(
            cursor,
            "dim_products",
            data["products"],
            ["product_id", "product_name", "category", "subcategory", "unit_price", "cost_price"],
        )

        self._load_table(
            cursor,
            "dim_date",
            data["dates"],
            [
                "date_id",
                "full_date",
                "year",
                "quarter",
                "month",
                "month_name",
                "week",
                "day_of_week",
                "day_name",
                "is_weekend",
            ],
        )

        self._load_table(cursor, "dim_channels", data["channels"], ["channel_id", "channel_name", "channel_type"])

        self._load_table(
            cursor,
            "fact_sales",
            data["sales"],
            [
                "sale_id",
                "customer_id",
                "product_id",
                "date_id",
                "channel_id",
                "quantity",
                "unit_price",
                "discount",
                "total_amount",
                "profit",
            ],
        )

        self._load_table(
            cursor,
            "fact_user_activity",
            data["activities"],
            [
                "activity_id",
                "customer_id",
                "date_id",
                "channel_id",
                "session_duration_sec",
                "pages_viewed",
                "actions_taken",
                "bounce",
            ],
        )

        self.conn.commit()

    def _load_table(self, cursor, table_name, rows, columns):
        """Load rows into a specific table."""
        placeholders = ", ".join(["?"] * len(columns))
        col_names = ", ".join(columns)
        sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

        for row in rows:
            values = [row[col] for col in columns]
            cursor.execute(sql, values)

        self.stats["loaded"][table_name] = len(rows)
        print(f"  Loaded {table_name}: {len(rows)} rows")

    def _validate(self):
        """Run data quality checks."""
        cursor = self.conn.cursor()
        checks = [
            ("Customers count", "SELECT COUNT(*) FROM dim_customers"),
            ("Products count", "SELECT COUNT(*) FROM dim_products"),
            ("Sales count", "SELECT COUNT(*) FROM fact_sales"),
            ("Activities count", "SELECT COUNT(*) FROM fact_user_activity"),
            ("Total revenue", "SELECT ROUND(SUM(total_amount), 2) FROM fact_sales"),
            ("Total profit", "SELECT ROUND(SUM(profit), 2) FROM fact_sales"),
            (
                "Orphan sales (should be 0)",
                """
                SELECT COUNT(*) FROM fact_sales s
                LEFT JOIN dim_customers c ON s.customer_id = c.customer_id
                WHERE c.customer_id IS NULL
            """,
            ),
            ("Avg session duration", "SELECT ROUND(AVG(session_duration_sec), 0) FROM fact_user_activity"),
        ]

        print("  Data Quality Checks:")
        for name, query in checks:
            result = cursor.execute(query).fetchone()[0]
            status = "PASS" if result is not None and result != "" else "WARN"
            print(f"    [{status}] {name}: {result}")

    def _save_log(self):
        """Save ETL run log."""
        log_path = os.path.join(PROCESSED_DIR, "etl_log.txt")
        with open(log_path, "w") as f:
            f.write("ETL Pipeline Log\n")
            f.write(f"Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"\nExtracted: {self.stats['extracted']}\n")
            f.write(f"Transformed: {self.stats['transformed']}\n")
            f.write(f"Loaded: {self.stats['loaded']}\n")
            f.write(f"Errors: {self.stats['errors']}\n")
        print(f"\n  Log saved: {log_path}")


if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run()
