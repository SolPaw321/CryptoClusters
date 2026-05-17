CREATE TABLE IF NOT EXISTS wallets (
    address VARCHAR(42) PRIMARY KEY,
    master_address VARCHAR(42),
    source VARCHAR(50),
    subaccount_name VARCHAR(100),
    CONSTRAINT fk_master_wallet
        FOREIGN KEY(master_address) 
        REFERENCES wallets(address)
);

CREATE TABLE IF NOT EXISTS wallet_processing_queue (
    wallet_address VARCHAR(42) PRIMARY KEY REFERENCES wallets(address),
    status VARCHAR(20) DEFAULT 'PENDING',
    worker_id VARCHAR(50),
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE INDEX idx_queue_pending ON wallet_processing_queue(status) WHERE status = 'PENDING';