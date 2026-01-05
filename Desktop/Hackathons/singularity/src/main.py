# src/main.py
import pathway as pw
import random
import uuid
import time

# Define the schema as a class
class TransactionSchema(pw.Schema):
    transaction_id: str
    user_id: str
    amount: float
    timestamp: int
    merchant: str
    country: str
    is_fraud: bool

MERCHANTS = ["Amazon", "Walmart", "Target", "BestBuy", "Costco", "Starbucks", "McDonalds"]
COUNTRIES = ["US", "UK", "IN", "CA", "AU", "DE", "FR"]

value_generators = {
    "transaction_id": lambda x: str(uuid.uuid4()),
    "user_id": lambda x: f"user_{random.randint(1, 1000)}",
    "amount": lambda x: round(random.uniform(1.0, 5000.0), 2),
    "timestamp": lambda x: int(time.time() * 1000),
    "merchant": lambda x: random.choice(MERCHANTS),
    "country": lambda x: random.choice(COUNTRIES),
    "is_fraud": lambda x: random.random() < 0.02,  # 2% fraud rate
}

# Generate realistic transaction stream
transactions = pw.demo.generate_custom_stream(
    value_generators,
    schema=TransactionSchema,
    nb_rows=None,  # Generate 100 rows for testing (set to None for infinite)
    autocommit_duration_ms=1000,
    input_rate=10.0,  # 10 transactions per second
)

# Save to CSV
pw.io.csv.write(transactions, "data/transactions.csv")

# Run the computation
pw.run()


