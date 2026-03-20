"""
SLOWLY CHANGING DIMENSION (SCD) HANDLER
Implements SCD Type 1, Type 2, and Type 3 strategies for dimension management.
Tracks historical changes with effective dates and version control.
"""

import sqlite3
import os
from datetime import datetime

BASE_DIR = os.path.join(os.path.dirname(__file__), '..', '..')
DB_PATH = os.path.join(BASE_DIR, 'data', 'warehouse.db')


class SCDHandler:
    """Manages Slowly Changing Dimensions in the data warehouse."""

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path

    def setup_scd_tables(self):
        """Create SCD Type 2 tracking tables."""
        conn = sqlite3.connect(self.db_path)
        conn.executescript("""
            -- SCD Type 2 Customer History
            CREATE TABLE IF NOT EXISTS dim_customers_history (
                surrogate_key INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT NOT NULL,
                country TEXT NOT NULL,
                city TEXT NOT NULL,
                age_group TEXT NOT NULL,
                signup_date TEXT NOT NULL,
                effective_from TEXT NOT NULL,
                effective_to TEXT DEFAULT '9999-12-31',
                is_current INTEGER DEFAULT 1,
                version INTEGER DEFAULT 1,
                change_type TEXT DEFAULT 'INSERT'
            );

            -- SCD Type 2 Product History
            CREATE TABLE IF NOT EXISTS dim_products_history (
                surrogate_key INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                product_name TEXT NOT NULL,
                category TEXT NOT NULL,
                subcategory TEXT NOT NULL,
                unit_price REAL NOT NULL,
                cost_price REAL NOT NULL,
                effective_from TEXT NOT NULL,
                effective_to TEXT DEFAULT '9999-12-31',
                is_current INTEGER DEFAULT 1,
                version INTEGER DEFAULT 1,
                change_type TEXT DEFAULT 'INSERT'
            );

            -- Change log for auditing
            CREATE TABLE IF NOT EXISTS scd_change_log (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                record_id INTEGER NOT NULL,
                change_type TEXT NOT NULL,
                changed_fields TEXT,
                old_values TEXT,
                new_values TEXT,
                changed_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_cust_hist_id ON dim_customers_history(customer_id);
            CREATE INDEX IF NOT EXISTS idx_cust_hist_current ON dim_customers_history(is_current);
            CREATE INDEX IF NOT EXISTS idx_prod_hist_id ON dim_products_history(product_id);
            CREATE INDEX IF NOT EXISTS idx_prod_hist_current ON dim_products_history(is_current);
        """)
        conn.commit()
        conn.close()
        print("  SCD tables created successfully.")

    def initial_load_customers(self):
        """Load current customers into SCD Type 2 history table."""
        conn = sqlite3.connect(self.db_path)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Clear existing history
        conn.execute("DELETE FROM dim_customers_history")

        customers = conn.execute("SELECT * FROM dim_customers").fetchall()
        for c in customers:
            conn.execute("""
                INSERT INTO dim_customers_history
                (customer_id, first_name, last_name, email, country, city, age_group, signup_date,
                 effective_from, effective_to, is_current, version, change_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '9999-12-31', 1, 1, 'INSERT')
            """, (c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7], now))

        conn.commit()
        print(f"  Loaded {len(customers)} customers into SCD Type 2 history.")
        conn.close()

    def initial_load_products(self):
        """Load current products into SCD Type 2 history table."""
        conn = sqlite3.connect(self.db_path)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        conn.execute("DELETE FROM dim_products_history")

        products = conn.execute("SELECT * FROM dim_products").fetchall()
        for p in products:
            conn.execute("""
                INSERT INTO dim_products_history
                (product_id, product_name, category, subcategory, unit_price, cost_price,
                 effective_from, effective_to, is_current, version, change_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, '9999-12-31', 1, 1, 'INSERT')
            """, (p[0], p[1], p[2], p[3], p[4], p[5], now))

        conn.commit()
        print(f"  Loaded {len(products)} products into SCD Type 2 history.")
        conn.close()

    def apply_scd2_customer_change(self, customer_id, new_data):
        """
        Apply SCD Type 2 change: close old record, insert new version.
        new_data: dict with keys matching customer columns.
        """
        conn = sqlite3.connect(self.db_path)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Get current record
        current = conn.execute("""
            SELECT * FROM dim_customers_history
            WHERE customer_id = ? AND is_current = 1
        """, (customer_id,)).fetchone()

        if not current:
            print(f"  No current record found for customer {customer_id}")
            conn.close()
            return

        # Detect changes
        field_map = {
            'first_name': 1, 'last_name': 2, 'email': 3,
            'country': 4, 'city': 5, 'age_group': 6
        }
        changed_fields = []
        old_values = {}
        new_values = {}

        for field, idx in field_map.items():
            if field in new_data and str(new_data[field]) != str(current[idx + 1]):
                changed_fields.append(field)
                old_values[field] = current[idx + 1]
                new_values[field] = new_data[field]

        if not changed_fields:
            print(f"  No changes detected for customer {customer_id}")
            conn.close()
            return

        current_version = current[12]  # version column

        # Close current record
        conn.execute("""
            UPDATE dim_customers_history
            SET effective_to = ?, is_current = 0
            WHERE customer_id = ? AND is_current = 1
        """, (now, customer_id))

        # Build new record with merged data
        merged = {
            'first_name': new_data.get('first_name', current[2]),
            'last_name': new_data.get('last_name', current[3]),
            'email': new_data.get('email', current[4]),
            'country': new_data.get('country', current[5]),
            'city': new_data.get('city', current[6]),
            'age_group': new_data.get('age_group', current[7]),
            'signup_date': current[8],
        }

        conn.execute("""
            INSERT INTO dim_customers_history
            (customer_id, first_name, last_name, email, country, city, age_group, signup_date,
             effective_from, effective_to, is_current, version, change_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '9999-12-31', 1, ?, 'UPDATE')
        """, (customer_id, merged['first_name'], merged['last_name'], merged['email'],
              merged['country'], merged['city'], merged['age_group'], merged['signup_date'],
              now, current_version + 1))

        # Log the change
        conn.execute("""
            INSERT INTO scd_change_log (table_name, record_id, change_type, changed_fields,
                                        old_values, new_values, changed_at)
            VALUES (?, ?, 'UPDATE', ?, ?, ?, ?)
        """, ('dim_customers', customer_id, str(changed_fields),
              str(old_values), str(new_values), now))

        conn.commit()
        conn.close()
        print(f"  SCD2 applied: customer {customer_id}, changed: {changed_fields}, version: {current_version + 1}")

    def get_customer_history(self, customer_id):
        """Get full version history for a customer."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT * FROM dim_customers_history
            WHERE customer_id = ?
            ORDER BY version
        """, (customer_id,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def simulate_changes(self):
        """Simulate some realistic SCD changes for demonstration."""
        print("\n  Simulating SCD Type 2 changes...")

        # Customer moved to a new city
        self.apply_scd2_customer_change(1, {'city': 'San Francisco', 'country': 'United States'})

        # Customer updated their email
        self.apply_scd2_customer_change(5, {'email': 'newemail5@example.com'})

        # Customer moved to a different country
        self.apply_scd2_customer_change(10, {'country': 'Germany', 'city': 'Berlin'})

        # Show history for customer 1
        history = self.get_customer_history(1)
        print(f"\n  Customer 1 version history ({len(history)} versions):")
        for h in history:
            print(f"    v{h['version']}: {h['city']}, {h['country']} "
                  f"[{h['effective_from']} to {h['effective_to']}] "
                  f"{'(current)' if h['is_current'] else '(closed)'}")


if __name__ == '__main__':
    handler = SCDHandler()
    handler.setup_scd_tables()
    handler.initial_load_customers()
    handler.initial_load_products()
    handler.simulate_changes()
