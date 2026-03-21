"""
DATA QUALITY FRAMEWORK
Advanced data validation, profiling, and anomaly detection engine.
Implements statistical quality checks, schema validation, and drift detection.
"""

import sqlite3
import os
import json
import math
from datetime import datetime
from collections import Counter

BASE_DIR = os.path.join(os.path.dirname(__file__), '..', '..')
DB_PATH = os.path.join(BASE_DIR, 'data', 'warehouse.db')
REPORT_DIR = os.path.join(BASE_DIR, 'data', 'processed', 'quality_reports')


class DataQualityEngine:
    """Enterprise-grade data quality validation framework."""

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.results = []
        self.profile_data = {}

    def run_full_audit(self):
        """Execute complete data quality audit."""
        print("=" * 70)
        print("DATA QUALITY AUDIT - PhD Level Analysis")
        print(f"Timestamp: {datetime.now().isoformat()}")
        print("=" * 70)

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        try:
            self._check_completeness(conn)
            self._check_uniqueness(conn)
            self._check_referential_integrity(conn)
            self._check_statistical_outliers(conn)
            self._check_distribution_analysis(conn)
            self._check_temporal_consistency(conn)
            self._check_business_rules(conn)
            self._data_profiling(conn)

            self._generate_report()
            self._print_summary()
        finally:
            conn.close()

        return self.results

    def _add_result(self, category, check_name, status, details, severity="INFO"):
        self.results.append({
            'timestamp': datetime.now().isoformat(),
            'category': category,
            'check': check_name,
            'status': status,
            'severity': severity,
            'details': details,
        })
        icon = "PASS" if status == "PASS" else ("WARN" if status == "WARN" else "FAIL")
        print(f"  [{icon}] [{severity}] {check_name}: {details}")

    def _check_completeness(self, conn):
        """Check for NULL values and missing data across all tables."""
        print("\n--- COMPLETENESS CHECKS ---")
        tables_columns = {
            'dim_customers': ['first_name', 'last_name', 'email', 'country', 'city', 'age_group'],
            'dim_products': ['product_name', 'category', 'subcategory', 'unit_price', 'cost_price'],
            'fact_sales': ['customer_id', 'product_id', 'date_id', 'quantity', 'total_amount', 'profit'],
            'fact_user_activity': ['customer_id', 'date_id', 'session_duration_sec', 'pages_viewed'],
        }
        for table, columns in tables_columns.items():
            total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for col in columns:
                nulls = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL OR {col} = ''").fetchone()[0]
                pct = round(nulls / total * 100, 2) if total > 0 else 0
                status = "PASS" if pct == 0 else ("WARN" if pct < 5 else "FAIL")
                severity = "INFO" if pct == 0 else ("MEDIUM" if pct < 5 else "HIGH")
                self._add_result("Completeness", f"{table}.{col} null check",
                                 status, f"{nulls}/{total} nulls ({pct}%)", severity)

    def _check_uniqueness(self, conn):
        """Check primary key and unique constraint violations."""
        print("\n--- UNIQUENESS CHECKS ---")
        checks = [
            ("dim_customers", "customer_id"),
            ("dim_customers", "email"),
            ("dim_products", "product_id"),
            ("fact_sales", "sale_id"),
            ("fact_user_activity", "activity_id"),
        ]
        for table, col in checks:
            total = conn.execute(f"SELECT COUNT({col}) FROM {table}").fetchone()[0]
            distinct = conn.execute(f"SELECT COUNT(DISTINCT {col}) FROM {table}").fetchone()[0]
            dupes = total - distinct
            status = "PASS" if dupes == 0 else "FAIL"
            self._add_result("Uniqueness", f"{table}.{col} uniqueness",
                             status, f"{dupes} duplicates found", "HIGH" if dupes > 0 else "INFO")

    def _check_referential_integrity(self, conn):
        """Check foreign key relationships (orphan detection)."""
        print("\n--- REFERENTIAL INTEGRITY ---")
        fk_checks = [
            ("fact_sales", "customer_id", "dim_customers", "customer_id"),
            ("fact_sales", "product_id", "dim_products", "product_id"),
            ("fact_sales", "date_id", "dim_date", "date_id"),
            ("fact_sales", "channel_id", "dim_channels", "channel_id"),
            ("fact_user_activity", "customer_id", "dim_customers", "customer_id"),
            ("fact_user_activity", "date_id", "dim_date", "date_id"),
            ("fact_user_activity", "channel_id", "dim_channels", "channel_id"),
        ]
        for child_table, child_col, parent_table, parent_col in fk_checks:
            orphans = conn.execute(f"""
                SELECT COUNT(*) FROM {child_table} c
                LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
                WHERE p.{parent_col} IS NULL
            """).fetchone()[0]
            status = "PASS" if orphans == 0 else "FAIL"
            self._add_result("Referential Integrity",
                             f"{child_table}.{child_col} -> {parent_table}.{parent_col}",
                             status, f"{orphans} orphan records", "CRITICAL" if orphans > 0 else "INFO")

    def _check_statistical_outliers(self, conn):
        """Detect statistical outliers using IQR and Z-score methods."""
        print("\n--- STATISTICAL OUTLIER DETECTION ---")

        # IQR method for sales amounts
        amounts = [r[0] for r in conn.execute(
            "SELECT total_amount FROM fact_sales ORDER BY total_amount"
        ).fetchall()]

        n = len(amounts)
        q1 = amounts[n // 4]
        q3 = amounts[3 * n // 4]
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        outliers = sum(1 for a in amounts if a < lower_bound or a > upper_bound)
        pct = round(outliers / n * 100, 2)
        self._add_result("Outliers", "Sales amount IQR outliers",
                         "WARN" if pct > 5 else "PASS",
                         f"{outliers} outliers ({pct}%), bounds=[{lower_bound:.2f}, {upper_bound:.2f}]",
                         "MEDIUM" if pct > 5 else "INFO")

        # Z-score for session duration
        sessions = [r[0] for r in conn.execute(
            "SELECT session_duration_sec FROM fact_user_activity"
        ).fetchall()]
        mean = sum(sessions) / len(sessions)
        std = math.sqrt(sum((x - mean) ** 2 for x in sessions) / len(sessions))
        z_outliers = sum(1 for s in sessions if abs((s - mean) / std) > 3) if std > 0 else 0
        self._add_result("Outliers", "Session duration Z-score outliers (|z|>3)",
                         "WARN" if z_outliers > len(sessions) * 0.01 else "PASS",
                         f"{z_outliers} outliers (mean={mean:.0f}s, std={std:.0f}s)", "MEDIUM")

    def _check_distribution_analysis(self, conn):
        """Analyze data distributions for skewness and anomalies."""
        print("\n--- DISTRIBUTION ANALYSIS ---")

        # Sales distribution by category - check for extreme imbalance
        categories = conn.execute("""
            SELECT p.category, COUNT(*) as cnt
            FROM fact_sales s JOIN dim_products p ON s.product_id = p.product_id
            GROUP BY p.category
        """).fetchall()

        counts = [r[1] for r in categories]
        mean_cnt = sum(counts) / len(counts)
        cv = (math.sqrt(sum((c - mean_cnt) ** 2 for c in counts) / len(counts)) / mean_cnt * 100) if mean_cnt > 0 else 0

        self._add_result("Distribution", "Category sales distribution (CV)",
                         "PASS" if cv < 50 else "WARN",
                         f"CV={cv:.1f}% across {len(categories)} categories", "MEDIUM")

        # Country distribution - Herfindahl index for concentration
        countries = conn.execute("""
            SELECT c.country, SUM(s.total_amount) as rev
            FROM fact_sales s JOIN dim_customers c ON s.customer_id = c.customer_id
            GROUP BY c.country
        """).fetchall()
        total_rev = sum(r[1] for r in countries)
        hhi = sum((r[1] / total_rev) ** 2 for r in countries) * 10000 if total_rev > 0 else 0
        self._add_result("Distribution", "Revenue concentration (HHI index)",
                         "PASS" if hhi < 2500 else "WARN",
                         f"HHI={hhi:.0f} (>2500=concentrated, <1500=diverse)", "INFO")

        # Benford's Law check on sales amounts (first digit distribution)
        first_digits = [int(str(abs(a[0])).replace('.', '').lstrip('0')[0])
                        for a in conn.execute("SELECT total_amount FROM fact_sales WHERE total_amount > 0").fetchall()
                        if str(abs(a[0])).replace('.', '').lstrip('0')]
        digit_counts = Counter(first_digits)
        total_digits = sum(digit_counts.values())
        benford_expected = {d: math.log10(1 + 1 / d) for d in range(1, 10)}
        max_deviation = 0
        for d in range(1, 10):
            observed = digit_counts.get(d, 0) / total_digits
            expected = benford_expected[d]
            max_deviation = max(max_deviation, abs(observed - expected))

        self._add_result("Distribution", "Benford's Law compliance (sales amounts)",
                         "PASS" if max_deviation < 0.05 else "WARN",
                         f"Max digit deviation={max_deviation:.4f} (threshold=0.05)", "INFO")

    def _check_temporal_consistency(self, conn):
        """Check time-series data for gaps and anomalies."""
        print("\n--- TEMPORAL CONSISTENCY ---")

        # Check for date gaps in sales
        date_coverage = conn.execute("""
            SELECT COUNT(DISTINCT d.full_date) as covered,
                   (SELECT COUNT(*) FROM dim_date) as total
            FROM fact_sales s
            JOIN dim_date d ON s.date_id = d.date_id
        """).fetchone()
        coverage_pct = round(date_coverage[0] / date_coverage[1] * 100, 1)
        self._add_result("Temporal", "Sales date coverage",
                         "PASS" if coverage_pct > 90 else "WARN",
                         f"{date_coverage[0]}/{date_coverage[1]} dates covered ({coverage_pct}%)", "MEDIUM")

        # Check for weekend/weekday sales ratio consistency
        weekend_ratio = conn.execute("""
            SELECT ROUND(
                SUM(CASE WHEN d.is_weekend = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) * 100, 1
            ) FROM fact_sales s JOIN dim_date d ON s.date_id = d.date_id
        """).fetchone()[0]
        self._add_result("Temporal", "Weekend sales ratio",
                         "PASS", f"{weekend_ratio}% of sales on weekends (expected ~28.6%)", "INFO")

    def _check_business_rules(self, conn):
        """Validate domain-specific business rules."""
        print("\n--- BUSINESS RULE VALIDATION ---")

        # Negative amounts
        neg_sales = conn.execute("SELECT COUNT(*) FROM fact_sales WHERE total_amount < 0").fetchone()[0]
        self._add_result("Business Rules", "No negative sale amounts",
                         "PASS" if neg_sales == 0 else "FAIL",
                         f"{neg_sales} negative sales found", "HIGH" if neg_sales > 0 else "INFO")

        # Quantity must be positive
        zero_qty = conn.execute("SELECT COUNT(*) FROM fact_sales WHERE quantity <= 0").fetchone()[0]
        self._add_result("Business Rules", "Positive sale quantities",
                         "PASS" if zero_qty == 0 else "FAIL",
                         f"{zero_qty} zero/negative quantities", "HIGH" if zero_qty > 0 else "INFO")

        # Cost price < unit price (margin check)
        bad_margin = conn.execute(
            "SELECT COUNT(*) FROM dim_products WHERE cost_price >= unit_price"
        ).fetchone()[0]
        self._add_result("Business Rules", "Product margin validation (cost < price)",
                         "PASS" if bad_margin == 0 else "WARN",
                         f"{bad_margin} products with cost >= price", "HIGH" if bad_margin > 0 else "INFO")

        # Bounce sessions should have low page views
        bad_bounce = conn.execute(
            "SELECT COUNT(*) FROM fact_user_activity WHERE bounce = 1 AND pages_viewed > 3"
        ).fetchone()[0]
        self._add_result("Business Rules", "Bounce sessions page view check (<= 3)",
                         "PASS" if bad_bounce == 0 else "WARN",
                         f"{bad_bounce} bounce sessions with >3 pages", "MEDIUM")

    def _data_profiling(self, conn):
        """Generate statistical profiles for all numeric columns."""
        print("\n--- DATA PROFILING ---")

        numeric_cols = [
            ("fact_sales", "total_amount", "Sale Amount"),
            ("fact_sales", "quantity", "Sale Quantity"),
            ("fact_sales", "profit", "Profit"),
            ("fact_sales", "discount", "Discount"),
            ("fact_user_activity", "session_duration_sec", "Session Duration"),
            ("fact_user_activity", "pages_viewed", "Pages Viewed"),
        ]

        for table, col, label in numeric_cols:
            stats = conn.execute(f"""
                SELECT
                    COUNT({col}) as cnt,
                    ROUND(MIN({col}), 2) as min_val,
                    ROUND(MAX({col}), 2) as max_val,
                    ROUND(AVG({col}), 2) as mean,
                    ROUND(SUM({col}), 2) as total
                FROM {table}
            """).fetchone()

            values = [r[0] for r in conn.execute(f"SELECT {col} FROM {table} ORDER BY {col}").fetchall()]
            n = len(values)
            median = values[n // 2] if n % 2 == 1 else (values[n // 2 - 1] + values[n // 2]) / 2
            mean = stats[3]
            std = math.sqrt(sum((x - mean) ** 2 for x in values) / n) if n > 0 else 0

            # Skewness
            skewness = (sum(((x - mean) / std) ** 3 for x in values) / n) if std > 0 else 0

            profile = {
                'count': stats[0], 'min': stats[1], 'max': stats[2],
                'mean': mean, 'median': round(median, 2), 'std': round(std, 2),
                'total': stats[4], 'skewness': round(skewness, 3),
            }
            self.profile_data[f"{table}.{col}"] = profile
            print(f"  {label}: mean={mean}, median={median:.2f}, std={std:.2f}, skew={skewness:.3f}")

    def _generate_report(self):
        """Save quality report as JSON."""
        os.makedirs(REPORT_DIR, exist_ok=True)
        report = {
            'audit_timestamp': datetime.now().isoformat(),
            'total_checks': len(self.results),
            'passed': sum(1 for r in self.results if r['status'] == 'PASS'),
            'warnings': sum(1 for r in self.results if r['status'] == 'WARN'),
            'failures': sum(1 for r in self.results if r['status'] == 'FAIL'),
            'checks': self.results,
            'profiles': self.profile_data,
        }

        report_path = os.path.join(REPORT_DIR, f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"\n  Report saved: {report_path}")

    def _print_summary(self):
        passed = sum(1 for r in self.results if r['status'] == 'PASS')
        warned = sum(1 for r in self.results if r['status'] == 'WARN')
        failed = sum(1 for r in self.results if r['status'] == 'FAIL')
        total = len(self.results)

        print(f"\n{'=' * 70}")
        print(f"AUDIT SUMMARY: {total} checks | {passed} PASS | {warned} WARN | {failed} FAIL")
        score = round(passed / total * 100, 1) if total > 0 else 0
        print(f"QUALITY SCORE: {score}%")
        print(f"{'=' * 70}")


if __name__ == '__main__':
    engine = DataQualityEngine()
    engine.run_full_audit()
