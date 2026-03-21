"""
DATA GENERATOR - Creates realistic synthetic data for the data warehouse.
Generates CSV files for: customers, products, channels, dates, sales, user activity.
"""

import csv
import os
import random
from datetime import datetime, timedelta

# Seed for reproducibility
random.seed(42)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw")

# --- Configuration ---
NUM_CUSTOMERS = 500
NUM_PRODUCTS = 50
NUM_SALES = 5000
NUM_ACTIVITIES = 8000
DATE_START = datetime(2023, 1, 1)
DATE_END = datetime(2024, 12, 31)

COUNTRIES = [
    ("United States", ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
    ("United Kingdom", ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"]),
    ("Germany", ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"]),
    ("Canada", ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"]),
    ("Australia", ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"]),
    ("Sri Lanka", ["Colombo", "Kandy", "Galle", "Jaffna", "Negombo"]),
    ("India", ["Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad"]),
    ("Japan", ["Tokyo", "Osaka", "Kyoto", "Yokohama", "Nagoya"]),
]

FIRST_NAMES = [
    "James",
    "Mary",
    "Robert",
    "Patricia",
    "John",
    "Jennifer",
    "Michael",
    "Linda",
    "David",
    "Elizabeth",
    "William",
    "Susan",
    "Richard",
    "Sarah",
    "Joseph",
    "Karen",
    "Thomas",
    "Lisa",
    "Charles",
    "Nancy",
    "Amal",
    "Priya",
    "Yuki",
    "Chen",
    "Fatima",
    "Omar",
    "Sofia",
    "Liam",
    "Emma",
    "Noah",
    "Dasun",
    "Kasun",
    "Nimali",
]

LAST_NAMES = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Wilson",
    "Anderson",
    "Taylor",
    "Thomas",
    "Moore",
    "Martin",
    "Lee",
    "White",
    "Kumar",
    "Tanaka",
    "Mueller",
    "Silva",
    "Perera",
    "Fernando",
    "Jayasuriya",
]

CATEGORIES = {
    "Electronics": {
        "subcategories": ["Laptops", "Smartphones", "Headphones", "Tablets", "Cameras"],
        "products": [
            "Pro Laptop",
            "Ultra Phone",
            "Wireless Earbuds",
            "Tablet Air",
            "DSLR Camera",
            "Gaming Laptop",
            "Budget Phone",
            "Noise-Cancel Headphones",
            "E-Reader",
            "Action Camera",
        ],
    },
    "Clothing": {
        "subcategories": ["Shirts", "Pants", "Shoes", "Jackets", "Accessories"],
        "products": [
            "Cotton T-Shirt",
            "Slim Jeans",
            "Running Shoes",
            "Winter Jacket",
            "Leather Belt",
            "Polo Shirt",
            "Cargo Pants",
            "Sneakers",
            "Rain Coat",
            "Sunglasses",
        ],
    },
    "Home & Garden": {
        "subcategories": ["Furniture", "Kitchen", "Decor", "Tools", "Lighting"],
        "products": [
            "Standing Desk",
            "Blender Pro",
            "Wall Art Set",
            "Drill Kit",
            "LED Floor Lamp",
            "Bookshelf",
            "Coffee Maker",
            "Throw Pillows",
            "Tool Box",
            "Smart Bulbs",
        ],
    },
    "Books": {
        "subcategories": ["Fiction", "Non-Fiction", "Technical", "Self-Help", "Education"],
        "products": [
            "Python Mastery",
            "Data Science Guide",
            "SQL Cookbook",
            "AI Revolution",
            "Clean Code",
            "Design Patterns",
            "Machine Learning",
            "Web Dev Handbook",
            "Cloud Computing",
            "DevOps Manual",
        ],
    },
    "Sports": {
        "subcategories": ["Fitness", "Outdoor", "Team Sports", "Water Sports", "Cycling"],
        "products": [
            "Yoga Mat",
            "Hiking Backpack",
            "Football",
            "Swim Goggles",
            "Mountain Bike",
            "Dumbbells Set",
            "Tent 4-Person",
            "Basketball",
            "Surfboard",
            "Cycling Helmet",
        ],
    },
}

CHANNELS = [
    (1, "Website", "Online"),
    (2, "Mobile App", "Online"),
    (3, "Retail Store", "Offline"),
    (4, "Partner Marketplace", "Online"),
    (5, "Social Media", "Online"),
    (6, "Call Center", "Offline"),
]

AGE_GROUPS = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]


def ensure_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def generate_dates():
    """Generate date dimension data."""
    dates = []
    date_id = 1
    current = DATE_START
    while current <= DATE_END:
        dates.append(
            {
                "date_id": date_id,
                "full_date": current.strftime("%Y-%m-%d"),
                "year": current.year,
                "quarter": (current.month - 1) // 3 + 1,
                "month": current.month,
                "month_name": current.strftime("%B"),
                "week": current.isocalendar()[1],
                "day_of_week": current.weekday(),
                "day_name": current.strftime("%A"),
                "is_weekend": 1 if current.weekday() >= 5 else 0,
            }
        )
        current += timedelta(days=1)
        date_id += 1
    return dates


def generate_customers():
    """Generate customer dimension data."""
    customers = []
    used_emails = set()
    for i in range(1, NUM_CUSTOMERS + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        email = f"{first.lower()}.{last.lower()}{i}@example.com"
        while email in used_emails:
            email = f"{first.lower()}.{last.lower()}{random.randint(100,999)}@example.com"
        used_emails.add(email)
        country, cities = random.choice(COUNTRIES)
        signup = DATE_START + timedelta(days=random.randint(0, (DATE_END - DATE_START).days))
        customers.append(
            {
                "customer_id": i,
                "first_name": first,
                "last_name": last,
                "email": email,
                "country": country,
                "city": random.choice(cities),
                "age_group": random.choice(AGE_GROUPS),
                "signup_date": signup.strftime("%Y-%m-%d"),
            }
        )
    return customers


def generate_products():
    """Generate product dimension data."""
    products = []
    pid = 1
    for category, info in CATEGORIES.items():
        for j, product_name in enumerate(info["products"]):
            cost = round(random.uniform(5, 500), 2)
            margin = random.uniform(1.2, 2.5)
            products.append(
                {
                    "product_id": pid,
                    "product_name": product_name,
                    "category": category,
                    "subcategory": info["subcategories"][j % len(info["subcategories"])],
                    "unit_price": round(cost * margin, 2),
                    "cost_price": cost,
                }
            )
            pid += 1
    return products


def generate_sales(customers, products, dates):
    """Generate fact sales data."""
    sales = []
    for i in range(1, NUM_SALES + 1):
        product = random.choice(products)
        date = random.choice(dates)
        qty = random.randint(1, 5)
        discount = random.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2])
        unit_price = product["unit_price"]
        total = round(qty * unit_price * (1 - discount), 2)
        profit = round(total - (qty * product["cost_price"]), 2)
        sales.append(
            {
                "sale_id": i,
                "customer_id": random.choice(customers)["customer_id"],
                "product_id": product["product_id"],
                "date_id": date["date_id"],
                "channel_id": random.choice(CHANNELS)[0],
                "quantity": qty,
                "unit_price": unit_price,
                "discount": discount,
                "total_amount": total,
                "profit": profit,
            }
        )
    return sales


def generate_user_activity(customers, dates):
    """Generate fact user activity data."""
    activities = []
    for i in range(1, NUM_ACTIVITIES + 1):
        bounce = 1 if random.random() < 0.3 else 0
        activities.append(
            {
                "activity_id": i,
                "customer_id": random.choice(customers)["customer_id"],
                "date_id": random.choice(dates)["date_id"],
                "channel_id": random.choice(CHANNELS)[0],
                "session_duration_sec": random.randint(10, 1800) if not bounce else random.randint(5, 30),
                "pages_viewed": random.randint(1, 20) if not bounce else 1,
                "actions_taken": random.randint(0, 15) if not bounce else 0,
                "bounce": bounce,
            }
        )
    return activities


def write_csv(filename, data, fieldnames):
    filepath = os.path.join(DATA_DIR, filename)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"  Generated {filepath} ({len(data)} rows)")


def main():
    print("=" * 50)
    print("DATA GENERATION - Starting...")
    print("=" * 50)

    ensure_dir()

    # Generate dimension data
    print("\n[1/6] Generating dates...")
    dates = generate_dates()
    write_csv("dim_dates.csv", dates, dates[0].keys())

    print("[2/6] Generating customers...")
    customers = generate_customers()
    write_csv("dim_customers.csv", customers, customers[0].keys())

    print("[3/6] Generating products...")
    products = generate_products()
    write_csv("dim_products.csv", products, products[0].keys())

    print("[4/6] Generating channels...")
    channels = [{"channel_id": c[0], "channel_name": c[1], "channel_type": c[2]} for c in CHANNELS]
    write_csv("dim_channels.csv", channels, ["channel_id", "channel_name", "channel_type"])

    # Generate fact data
    print("[5/6] Generating sales transactions...")
    sales = generate_sales(customers, products, dates)
    write_csv("fact_sales.csv", sales, sales[0].keys())

    print("[6/6] Generating user activity...")
    activities = generate_user_activity(customers, dates)
    write_csv("fact_user_activity.csv", activities, activities[0].keys())

    print("\n" + "=" * 50)
    print("DATA GENERATION COMPLETE!")
    print(f"  Customers: {len(customers)}")
    print(f"  Products:  {len(products)}")
    print(f"  Sales:     {len(sales)}")
    print(f"  Activities:{len(activities)}")
    print("=" * 50)


if __name__ == "__main__":
    main()
