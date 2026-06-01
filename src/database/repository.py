import datetime
from typing import List, Any

def fetch_pending_batch(conn, batch_size: int) -> List[str]:
    """
    Fetch batch addresses, change their status to PROCESSING, 
    and set started_at timestamp.
    """
    query = """
        UPDATE wallet_processing_queue
        SET status = 'PROCESSING',
            started_at = NOW(),
            completed_at = NULL
        WHERE wallet_address IN (
            SELECT wallet_address
            FROM wallet_processing_queue
            WHERE status = 'PENDING'
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        RETURNING wallet_address;
    """
    with conn.cursor() as cur:
        cur.execute(query, (batch_size,))
        addresses = [row[0] for row in cur.fetchall()]
    
    conn.commit()
    return addresses

def mark_batch_as_done(conn, addresses: List[str]) -> None:
    """
    Changes processed addresses status to DONE 
    and updates completed_at with the completion timestamp.
    """
    if not addresses:
        return
    query = """
        UPDATE wallet_processing_queue
        SET status = 'DONE',
            completed_at = NOW()
        WHERE wallet_address = %s;
    """
    with conn.cursor() as cur:
        cur.executemany(query, [(addr,) for addr in addresses])

def revert_batch_to_pending(conn, addresses: List[str]) -> None:
    """
    Reverts status of unprocessed addresses from PROCESSING to PENDING 
    and clears timestamps for clean retry.
    """
    if not addresses:
        return
    query = """
        UPDATE wallet_processing_queue
        SET status = 'PENDING',
            started_at = NULL,
            completed_at = NULL
        WHERE wallet_address = %s;
    """
    with conn.cursor() as cur:
        cur.executemany(query, [(addr,) for addr in addresses])
