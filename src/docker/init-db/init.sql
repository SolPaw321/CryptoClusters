-- Safely create the ENUM type for sync priority if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'sync_priority') THEN
        CREATE TYPE sync_priority AS ENUM ('VIP', 'HIGH', 'LOW', 'IGNORED');
    END IF;
END
$$;

-- Main wallets table storing hierarchical relationships master-subaccount
CREATE TABLE IF NOT EXISTS wallets (
    address VARCHAR(42) PRIMARY KEY,
    master_address VARCHAR(42),
    source VARCHAR(50),
    subaccount_name VARCHAR(100),
    CONSTRAINT fk_master_wallet
        FOREIGN KEY(master_address) 
        REFERENCES wallets(address)
);

-- Queue for initial subaccount resolution
CREATE TABLE IF NOT EXISTS wallet_processing_queue (
    wallet_address VARCHAR(42) PRIMARY KEY REFERENCES wallets(address),
    status VARCHAR(20) DEFAULT 'PENDING',
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_queue_pending ON wallet_processing_queue(status) WHERE status = 'PENDING';

-- Continuous sync queue for the Radar/Sniper architecture
CREATE TABLE IF NOT EXISTS wallet_data_sync_queue (
    wallet_address VARCHAR(42) PRIMARY KEY REFERENCES wallets(address) ON DELETE CASCADE,
    
    priority sync_priority DEFAULT 'HIGH',
    status VARCHAR(20) DEFAULT 'IDLE',
    
    next_scan_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Pagination markers to prevent fetching duplicate data
    last_fills_timestamp BIGINT DEFAULT 0,
    last_funding_timestamp BIGINT DEFAULT 0,
    
    force_deep_sync BOOLEAN DEFAULT FALSE,
    reason_ignored VARCHAR(50)
);

-- Index optimizing the worker's search for actionable wallets, skipping ignored ones
CREATE INDEX IF NOT EXISTS idx_sync_queue_ready ON wallet_data_sync_queue(status, next_scan_at) 
WHERE priority != 'IGNORED';

-- 1:N relation storing the historical equity curve directly from clearinghouseState
CREATE TABLE IF NOT EXISTS clearinghouse_state_history (
    wallet_address VARCHAR(42) REFERENCES wallets(address) ON DELETE CASCADE,
    snapshot_time TIMESTAMPTZ DEFAULT NOW(),
    
    account_value NUMERIC NOT NULL,
    total_margin_used NUMERIC NOT NULL DEFAULT 0,
    withdrawable NUMERIC NOT NULL DEFAULT 0,
    
    raw_state JSONB NOT NULL,

    PRIMARY KEY (wallet_address, snapshot_time)
);

-- Crucial index for quickly fetching the most recent state or historical peaks
CREATE INDEX IF NOT EXISTS idx_clearinghouse_history_recent ON clearinghouse_state_history(wallet_address, snapshot_time DESC);

-- 1:N relation for detailed transaction history
CREATE TABLE IF NOT EXISTS user_fills (
    trade_id BIGINT PRIMARY KEY,
    wallet_address VARCHAR(42) REFERENCES wallets(address) ON DELETE CASCADE,
    
    coin VARCHAR(10) NOT NULL,
    direction VARCHAR(20) NOT NULL,
    side VARCHAR(1) NOT NULL,
    crossed BOOLEAN NOT NULL,
    order_id BIGINT NOT NULL,
    
    price NUMERIC NOT NULL,
    size NUMERIC NOT NULL,
    start_position NUMERIC NOT NULL,
    closed_pnl NUMERIC NOT NULL,
    fee NUMERIC NOT NULL,
    fee_token VARCHAR(20) DEFAULT 'USDC',
    tx_hash VARCHAR(66),
    
    trade_time TIMESTAMPTZ NOT NULL
);

-- Indexes for querying transaction history by wallet and grouping partial fills
CREATE INDEX IF NOT EXISTS idx_fills_wallet_time ON user_fills(wallet_address, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_fills_order ON user_fills(order_id);

-- 1:N relation for funding fee payments
CREATE TABLE IF NOT EXISTS user_funding (
    id BIGSERIAL PRIMARY KEY,
    wallet_address VARCHAR(42) REFERENCES wallets(address) ON DELETE CASCADE,
    
    coin VARCHAR(10) NOT NULL,
    funding_rate NUMERIC NOT NULL,
    position_size NUMERIC NOT NULL,
    amount_usdc NUMERIC NOT NULL,
    tx_hash VARCHAR(66),
    
    funding_time TIMESTAMPTZ NOT NULL,
    
    -- Constraint preventing duplicate funding records for the same epoch
    UNIQUE (wallet_address, coin, funding_time)
);

-- Index for retrieving funding history chronologically
CREATE INDEX IF NOT EXISTS idx_funding_wallet_time ON user_funding(wallet_address, funding_time DESC);

-- 1:N relation for high-resolution equity curves fetched via deep scans
CREATE TABLE IF NOT EXISTS portfolio_history (
    wallet_address VARCHAR(42) REFERENCES wallets(address) ON DELETE CASCADE,
    snapshot_time TIMESTAMPTZ NOT NULL,
    
    account_value NUMERIC NOT NULL,
    pnl NUMERIC NOT NULL,
    
    PRIMARY KEY (wallet_address, snapshot_time)
);