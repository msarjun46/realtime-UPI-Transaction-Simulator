"""
The "Living" Data Generator: Simulating the UPI Pulse
------------------------------------------------------
This isn't just a random script. It's designed to mimic the actual 
behavior of millions of Indian users and merchants.
"""

import random
import uuid
from datetime import datetime
import json

def generate_transaction():
    """
    Creates a transaction that feels 'real' to a Data Scientist.
    """
    
    # --- The Identity of a Transaction ---
    # Every swipe needs a globally unique ID (TXN...)
    transaction_id = f"TXN{uuid.uuid4().hex[:12].upper()}"
    now = datetime.now()
    
    # --- Success vs Failure Logic ---
    # In the real world, most UPI transactions succeed (85%+).
    # We simulate 'failures' and 'revoked' payments to test our cleaning logic.
    transaction_status = random.choices(
        ["SUCCESS", "FAILED", "PENDING", "REVOKED"], 
        weights=[0.85, 0.08, 0.05, 0.02]
    )[0]
    
    # --- The Amount (The 'Chai' to 'Laptop' scale) ---
    # Most UPI transactions are small (P2P/P2M). 
    amount = round(random.uniform(1.0, 5000.0), 2)
    
    # --- Geo-Location (Mapping the India Story) ---
    # We generate Lat/Long coordinates within India's borders 
    # to allow for beautiful Map Visualizations in Power BI.
    lat = round(random.uniform(8.4, 37.6), 6)
    lon = round(random.uniform(68.7, 97.2), 6)

    # --- Risk Intelligence ---
    # Every modern bank has a Fraud Score. 
    # We flag anything over 0.85 to test our 'Flagging' logic in Spark.
    fraud_score = round(random.uniform(0.0, 1.0), 4)
    is_flagged = True if fraud_score > 0.85 else False

    record = {
        "transaction_id": transaction_id,
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "amount": amount,
        "transaction_status": transaction_status,
        "fraud_score": fraud_score,
        "is_flagged": is_flagged,
        "latitude": lat,
        "longitude": lon,
        "payer_name": f"User_{random.randint(100, 999)}",
        "merchant_name": random.choice(["Zomato", "Amazon", "Starbucks", "Jio"]),
        "device_id": f"DEV-{uuid.uuid4().hex[:8].upper()}",
        "streamed_at": now.isoformat()
    }

    # --- The Reversal Story (Refunds) ---
    # Refunds only exist if the original transaction didn't go well.
    if transaction_status in ["FAILED", "PENDING"] and random.random() < 0.3:
        record["refund_request_id"] = f"REF{uuid.uuid4().hex[:12].upper()}"
        record["refund_amount"] = amount
        record["refund_status"] = "INITIATED"

    return record

if __name__ == "__main__":
    print(" Generating a sample Transaction...")
    print(json.dumps(generate_transaction(), indent=4))
