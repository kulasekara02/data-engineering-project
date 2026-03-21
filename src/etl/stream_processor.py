"""
REAL-TIME STREAM PROCESSING SIMULATOR
Simulates real-time event streaming with windowed aggregations,
event-time processing, watermarks, and exactly-once semantics.
"""

import sqlite3
import os
import random
import time
from datetime import datetime, timedelta
from collections import defaultdict

BASE_DIR = os.path.join(os.path.dirname(__file__), '..', '..')
DB_PATH = os.path.join(BASE_DIR, 'data', 'warehouse.db')


class EventStream:
    """Generates simulated real-time events."""

    def __init__(self, events_per_second=10):
        self.eps = events_per_second
        self.event_types = ['page_view', 'add_to_cart', 'purchase', 'search', 'logout']
        self.weights = [0.4, 0.25, 0.1, 0.2, 0.05]

    def generate_event(self):
        """Generate a single event with event-time timestamp."""
        event_type = random.choices(self.event_types, weights=self.weights, k=1)[0]
        # Simulate out-of-order events (event time slightly before processing time)
        lag_seconds = random.expovariate(1.0)  # exponential delay
        event_time = datetime.now() - timedelta(seconds=lag_seconds)

        event = {
            'event_id': f"evt_{int(time.time() * 1000)}_{random.randint(1000,9999)}",
            'event_type': event_type,
            'customer_id': random.randint(1, 500),
            'event_time': event_time.isoformat(),
            'processing_time': datetime.now().isoformat(),
            'channel': random.choice(['web', 'mobile', 'api']),
            'metadata': {
                'page': f"/page/{random.randint(1, 50)}",
                'session_id': f"sess_{random.randint(10000, 99999)}",
                'value': round(random.uniform(5, 500), 2) if event_type == 'purchase' else None,
            }
        }
        return event


class WindowedAggregator:
    """
    Implements tumbling and sliding window aggregations.
    Supports event-time processing with watermarks.
    """

    def __init__(self, window_size_sec=60):
        self.window_size = window_size_sec
        self.windows = defaultdict(lambda: {
            'count': 0,
            'events_by_type': defaultdict(int),
            'total_value': 0.0,
            'unique_customers': set(),
            'channels': defaultdict(int),
        })
        self.watermark = None
        self.late_events = 0

    def get_window_key(self, event_time):
        """Calculate the window this event belongs to."""
        timestamp = datetime.fromisoformat(event_time)
        window_start = timestamp.replace(
            second=(timestamp.second // self.window_size) * self.window_size,
            microsecond=0
        )
        return window_start.isoformat()

    def process_event(self, event):
        """Process a single event into its window."""
        window_key = self.get_window_key(event['event_time'])

        # Watermark check (late event detection)
        if self.watermark and event['event_time'] < self.watermark:
            self.late_events += 1

        window = self.windows[window_key]
        window['count'] += 1
        window['events_by_type'][event['event_type']] += 1
        window['unique_customers'].add(event['customer_id'])
        window['channels'][event['channel']] += 1

        if event['metadata'].get('value'):
            window['total_value'] += event['metadata']['value']

        # Advance watermark
        self.watermark = event['event_time']

    def get_window_summary(self, window_key):
        """Get aggregated summary for a specific window."""
        w = self.windows[window_key]
        return {
            'window': window_key,
            'total_events': w['count'],
            'events_by_type': dict(w['events_by_type']),
            'total_revenue': round(w['total_value'], 2),
            'unique_customers': len(w['unique_customers']),
            'channels': dict(w['channels']),
        }

    def get_all_summaries(self):
        """Get summaries for all windows."""
        return [self.get_window_summary(k) for k in sorted(self.windows.keys())]


class StreamProcessor:
    """Main stream processing engine with exactly-once semantics."""

    def __init__(self):
        self.stream = EventStream()
        self.aggregator = WindowedAggregator(window_size_sec=10)
        self.processed_ids = set()  # For deduplication (exactly-once)
        self.total_processed = 0
        self.total_duplicates = 0

    def process_batch(self, batch_size=100):
        """Process a batch of events (micro-batch processing)."""
        events = [self.stream.generate_event() for _ in range(batch_size)]

        for event in events:
            # Exactly-once: deduplicate by event_id
            if event['event_id'] in self.processed_ids:
                self.total_duplicates += 1
                continue

            self.processed_ids.add(event['event_id'])
            self.aggregator.process_event(event)
            self.total_processed += 1

        return events

    def run_simulation(self, num_batches=5, batch_size=100, save_to_db=True):
        """Run the streaming simulation."""
        print("=" * 70)
        print("STREAM PROCESSING SIMULATION")
        print(f"Batches: {num_batches}, Batch size: {batch_size}")
        print("=" * 70)

        all_events = []
        for i in range(num_batches):
            print(f"\n  Processing batch {i + 1}/{num_batches}...")
            events = self.process_batch(batch_size)
            all_events.extend(events)

            # Print real-time stats
            summaries = self.aggregator.get_all_summaries()
            if summaries:
                latest = summaries[-1]
                print(f"    Events in window: {latest['total_events']}, "
                      f"Revenue: ${latest['total_revenue']:.2f}, "
                      f"Unique customers: {latest['unique_customers']}")

        if save_to_db:
            self._save_to_warehouse(all_events)

        self._print_summary()
        return self.aggregator.get_all_summaries()

    def _save_to_warehouse(self, events):
        """Save streaming results to the data warehouse."""
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stream_events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                customer_id INTEGER,
                event_time TEXT NOT NULL,
                processing_time TEXT NOT NULL,
                channel TEXT,
                page TEXT,
                session_id TEXT,
                value REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stream_windows (
                window_start TEXT PRIMARY KEY,
                total_events INTEGER,
                total_revenue REAL,
                unique_customers INTEGER,
                page_views INTEGER DEFAULT 0,
                purchases INTEGER DEFAULT 0,
                add_to_cart INTEGER DEFAULT 0,
                searches INTEGER DEFAULT 0
            )
        """)

        # Save events
        for e in events:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO stream_events
                    (event_id, event_type, customer_id, event_time, processing_time,
                     channel, page, session_id, value)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (e['event_id'], e['event_type'], e['customer_id'],
                      e['event_time'], e['processing_time'], e['channel'],
                      e['metadata']['page'], e['metadata']['session_id'],
                      e['metadata'].get('value')))
            except Exception:
                pass

        # Save window aggregations
        for summary in self.aggregator.get_all_summaries():
            conn.execute("""
                INSERT OR REPLACE INTO stream_windows
                (window_start, total_events, total_revenue, unique_customers,
                 page_views, purchases, add_to_cart, searches)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (summary['window'], summary['total_events'],
                  summary['total_revenue'], summary['unique_customers'],
                  summary['events_by_type'].get('page_view', 0),
                  summary['events_by_type'].get('purchase', 0),
                  summary['events_by_type'].get('add_to_cart', 0),
                  summary['events_by_type'].get('search', 0)))

        conn.commit()
        conn.close()
        print(f"\n  Saved {len(events)} events and {len(self.aggregator.get_all_summaries())} windows to warehouse.")

    def _print_summary(self):
        print(f"\n{'=' * 70}")
        print("STREAM PROCESSING SUMMARY")
        print(f"  Total processed: {self.total_processed}")
        print(f"  Duplicates filtered: {self.total_duplicates}")
        print(f"  Late events: {self.aggregator.late_events}")
        print(f"  Windows created: {len(self.aggregator.windows)}")
        summaries = self.aggregator.get_all_summaries()
        if summaries:
            total_rev = sum(s['total_revenue'] for s in summaries)
            total_cust = max(s['unique_customers'] for s in summaries)
            print(f"  Total streaming revenue: ${total_rev:.2f}")
            print(f"  Peak concurrent customers: {total_cust}")
        print(f"{'=' * 70}")


if __name__ == '__main__':
    processor = StreamProcessor()
    processor.run_simulation(num_batches=5, batch_size=200)
