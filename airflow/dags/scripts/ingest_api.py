"""
Data Ingestion — Fetch from DummyJSON API + enrich with Faker.

Loads into PostgreSQL raw schema:
  - raw.products  (~194 rows from API)
  - raw.users     (~208 rows from API)
  - raw.orders    (~2,000+ rows, API carts + Faker-generated)
"""

import json
import random
import logging
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker
from sqlalchemy import text

from scripts.utils import get_engine, fetch_api_data

logger = logging.getLogger("ecommerce_pipeline")
fake = Faker()
Faker.seed(42)
random.seed(42)

PRODUCT_CATEGORIES = [
    "beauty", "fragrances", "furniture", "groceries",
    "home-decoration", "kitchen-accessories", "laptops",
    "mens-shirts", "mens-shoes", "mens-watches",
    "mobile-accessories", "motorcycle", "skin-care",
    "smartphones", "sports-accessories", "sunglasses",
    "tablets", "tops", "vehicle", "womens-bags",
    "womens-dresses", "womens-jewellery", "womens-shoes", "womens-watches",
]

BRANDS = [
    "Essence", "Glamour Beauty", "Velvet Touch", "Apple", "Samsung",
    "Sony", "Nike", "Adidas", "HP", "Dell", "Calvin Klein", "Gucci",
]


def ingest_products():
    """Fetch products from DummyJSON and load into raw.products."""
    logger.info("Starting products ingestion...")
    products_raw = fetch_api_data("products")

    if not products_raw:
        logger.warning("API unavailable. Using Faker fallback.")
        products_raw = _generate_fake_products(200)

    products = []
    for p in products_raw:
        products.append({
            "id": p.get("id"),
            "title": p.get("title"),
            "description": p.get("description"),
            "category": p.get("category"),
            "price": p.get("price"),
            "discount_percentage": p.get("discountPercentage", 0),
            "rating": p.get("rating"),
            "stock": p.get("stock"),
            "brand": p.get("brand", "Unknown"),
            "sku": p.get("sku"),
            "weight": p.get("weight"),
            "warranty_information": p.get("warrantyInformation"),
            "shipping_information": p.get("shippingInformation"),
            "availability_status": p.get("availabilityStatus"),
            "return_policy": p.get("returnPolicy"),
            "minimum_order_quantity": p.get("minimumOrderQuantity"),
            "thumbnail": p.get("thumbnail"),
            "images": json.dumps(p.get("images", [])),
            "tags": json.dumps(p.get("tags", [])),
        })

    df = pd.DataFrame(products)
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE raw.products RESTART IDENTITY"))
    df.to_sql("products", engine, schema="raw", if_exists="append", index=False)

    logger.info(f"Loaded {len(df)} products into raw.products")
    return len(df)


def ingest_users():
    """Fetch users from DummyJSON and load into raw.users."""
    logger.info("Starting users ingestion...")
    users_raw = fetch_api_data("users")

    if not users_raw:
        logger.warning("API unavailable. Using Faker fallback.")
        users_raw = _generate_fake_users(300)

    users = []
    for u in users_raw:
        address = u.get("address", {})
        company = u.get("company", {})
        users.append({
            "id": u.get("id"),
            "first_name": u.get("firstName"),
            "last_name": u.get("lastName"),
            "maiden_name": u.get("maidenName", ""),
            "age": u.get("age"),
            "gender": u.get("gender"),
            "email": u.get("email"),
            "phone": u.get("phone"),
            "username": u.get("username"),
            "birth_date": u.get("birthDate"),
            "address_street": address.get("address", ""),
            "address_city": address.get("city", ""),
            "address_state": address.get("state", ""),
            "address_postal_code": address.get("postalCode", ""),
            "address_country": address.get("country", "United States"),
            "company_name": company.get("name", ""),
            "company_title": company.get("title", ""),
            "company_department": company.get("department", ""),
            "university": u.get("university", ""),
        })

    df = pd.DataFrame(users)
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE raw.users RESTART IDENTITY"))
    df.to_sql("users", engine, schema="raw", if_exists="append", index=False)

    logger.info(f"Loaded {len(df)} users into raw.users")
    return len(df)


def ingest_orders():
    """
    Fetch carts from DummyJSON, flatten into order line items,
    then enrich with Faker to reach 2,000+ rows for time-series analysis.
    """
    logger.info("Starting orders ingestion...")
    carts_raw = fetch_api_data("carts")

    # Step 1: Flatten API carts into order line items
    orders = []
    if carts_raw:
        for cart in carts_raw:
            cart_id = cart.get("id")
            user_id = cart.get("userId")
            order_date = _random_date_last_year()

            for product in cart.get("products", []):
                orders.append({
                    "cart_id": cart_id,
                    "user_id": user_id,
                    "product_id": product.get("id"),
                    "product_title": product.get("title"),
                    "price": product.get("price"),
                    "quantity": product.get("quantity"),
                    "total": product.get("total"),
                    "discount_percentage": product.get("discountPercentage", 0),
                    "discounted_total": product.get("discountedTotal"),
                    "order_date": order_date,
                })

    logger.info(f"Processed {len(orders)} order lines from API carts")

    # Step 2: Enrich with Faker-generated orders to reach target
    engine = get_engine()
    try:
        existing_products = pd.read_sql("SELECT id, title, price FROM raw.products", engine)
        product_list = existing_products.to_dict("records")
    except Exception:
        product_list = [
            {"id": i, "title": f"Product {i}", "price": round(random.uniform(10, 500), 2)}
            for i in range(1, 195)
        ]

    try:
        existing_users = pd.read_sql("SELECT id FROM raw.users", engine)
        user_ids = existing_users["id"].tolist()
    except Exception:
        user_ids = list(range(1, 209))

    target_total = 2000
    extra_needed = max(0, target_total - len(orders))
    cart_id_counter = 100

    logger.info(f"Generating {extra_needed} additional fake order lines...")

    for _ in range(extra_needed // 3):
        cart_id_counter += 1
        user_id = random.choice(user_ids)
        order_date = _random_date_last_year()
        num_items = random.randint(1, 5)

        for product in random.sample(product_list, min(num_items, len(product_list))):
            quantity = random.randint(1, 5)
            price = product["price"]
            total = round(price * quantity, 2)
            discount_pct = round(random.uniform(0, 25), 2)
            discounted_total = round(total * (1 - discount_pct / 100), 2)

            orders.append({
                "cart_id": cart_id_counter,
                "user_id": user_id,
                "product_id": product["id"],
                "product_title": product["title"],
                "price": price,
                "quantity": quantity,
                "total": total,
                "discount_percentage": discount_pct,
                "discounted_total": discounted_total,
                "order_date": order_date,
            })

    df = pd.DataFrame(orders)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE raw.orders RESTART IDENTITY"))
    df.to_sql("orders", engine, schema="raw", if_exists="append", index=False)

    logger.info(f"Loaded {len(df)} order lines into raw.orders")
    return len(df)


# --- Faker fallback generators ---

def _random_date_last_year() -> str:
    """Generate a random date within the last 365 days."""
    days_ago = random.randint(0, 365)
    return (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def _generate_fake_products(count: int) -> list:
    """Generate fake products when API is unavailable."""
    products = []
    for i in range(1, count + 1):
        cat = random.choice(PRODUCT_CATEGORIES)
        products.append({
            "id": i,
            "title": fake.catch_phrase(),
            "description": fake.paragraph(nb_sentences=2),
            "category": cat,
            "price": round(random.uniform(5.0, 2000.0), 2),
            "discountPercentage": round(random.uniform(0, 30), 2),
            "rating": round(random.uniform(1.0, 5.0), 2),
            "stock": random.randint(0, 200),
            "brand": random.choice(BRANDS),
            "sku": fake.bothify(text="???-###-???-###").upper(),
            "weight": round(random.uniform(0.1, 50.0), 2),
            "warrantyInformation": random.choice([
                "1 month warranty", "6 months warranty",
                "1 year warranty", "2 year warranty", "No warranty",
            ]),
            "shippingInformation": random.choice([
                "Ships in 1-2 business days", "Ships in 3-5 business days",
                "Ships in 1 week", "Ships in 2 weeks",
            ]),
            "availabilityStatus": random.choice(["In Stock", "Low Stock", "Out of Stock"]),
            "returnPolicy": random.choice([
                "No return policy", "7 days return policy",
                "30 days return policy", "60 days return policy",
            ]),
            "minimumOrderQuantity": random.randint(1, 50),
            "thumbnail": f"https://placehold.co/200x200?text=Product+{i}",
            "images": [f"https://placehold.co/600x400?text=Product+{i}"],
            "tags": [cat, fake.word()],
        })
    return products


def _generate_fake_users(count: int) -> list:
    """Generate fake users when API is unavailable."""
    users = []
    for i in range(1, count + 1):
        users.append({
            "id": i,
            "firstName": fake.first_name(),
            "lastName": fake.last_name(),
            "maidenName": fake.last_name() if random.random() > 0.5 else "",
            "age": random.randint(18, 65),
            "gender": random.choice(["male", "female"]),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "username": fake.user_name(),
            "birthDate": fake.date_of_birth(minimum_age=18, maximum_age=65).isoformat(),
            "address": {
                "address": fake.street_address(),
                "city": fake.city(),
                "state": fake.state(),
                "postalCode": fake.zipcode(),
                "country": "United States",
            },
            "company": {
                "name": fake.company(),
                "title": fake.job(),
                "department": random.choice([
                    "Engineering", "Marketing", "Sales", "Support",
                    "HR", "Finance", "Operations",
                ]),
            },
            "university": fake.company() + " University",
        })
    return users


def run_full_ingestion():
    """Execute complete ingestion pipeline."""
    logger.info("Starting full data ingestion pipeline...")
    results = {
        "products": ingest_products(),
        "users": ingest_users(),
        "orders": ingest_orders(),
    }
    logger.info(f"Ingestion completed: {results}")
    return results


if __name__ == "__main__":
    run_full_ingestion()
