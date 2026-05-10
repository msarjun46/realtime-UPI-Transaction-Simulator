-- CORE TRANSACTIONS
CREATE TABLE IF NOT EXISTS upi_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    date DATE,
    time TIME,
    amount DECIMAL(15, 2),
    transaction_status VARCHAR(20),
    fraud_score DECIMAL(5, 4),
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6),
    device_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- MERCHANT DATA
CREATE TABLE IF NOT EXISTS upi_merchant_transactions (
    merchant_txn_id VARCHAR(50) PRIMARY KEY,
    transaction_id VARCHAR(50) REFERENCES upi_transactions(transaction_id),
    merchant_name VARCHAR(100),
    merchant_category_code CHAR(4)
);

-- REFUNDS
CREATE TABLE IF NOT EXISTS upi_refunds (
    refund_request_id VARCHAR(50) PRIMARY KEY,
    transaction_id VARCHAR(50) REFERENCES upi_transactions(transaction_id),
    refund_amount DECIMAL(15, 2),
    refund_status VARCHAR(20)
);

-- GOLD KPIs
CREATE TABLE IF NOT EXISTS upi_transaction_gold (
    hour INT,
    transaction_status VARCHAR(20),
    total_transactions INT,
    total_volume DECIMAL(20, 2),
    PRIMARY KEY (hour, transaction_status)
);
