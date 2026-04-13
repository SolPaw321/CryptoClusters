from src.addresses import TradesSource, TransactionSink
from src.google_drive import GoogleDriveHandler, GdPaths
from src.misc.csv import unique_rows
from src.paths import PATHS

if __name__ == "__main__":
    UNIQUE_WALLETS_TEMP = PATHS.ADDRESSES_TEMP / "unique_wallets.csv"
    UNIQUE_WALLETS = PATHS.ADDRESSES / "unique_wallets.csv"

    gd = GoogleDriveHandler()
    if not UNIQUE_WALLETS.exists():
        gd.download_folder(
            google_drive_folder_id=GdPaths.ADDRESSES,
            local_folder_path=PATHS.ADDRESSES,
            rewrite=False
        )
    gd.download_folder(
        google_drive_folder_id=GdPaths.ADDRESSES,
        local_folder_path=PATHS.ADDRESSES_TEMP,
        rewrite=True
    )

    unique_rows(
        UNIQUE_WALLETS,
        UNIQUE_WALLETS_TEMP,
        "address",
        output_file=UNIQUE_WALLETS,
        files_to_delete=UNIQUE_WALLETS_TEMP,
        name="main"
    )


    trade_wallets_temp = PATHS.ADDRESSES_TEMP / "trade_wallets_temp.csv"

    sink = TransactionSink(csv_path=trade_wallets_temp)

    source = TradesSource(
        sink=sink,
        markets=["BTC", "ETH", "SOL", "XRP"],
        max_running_time=60000.0,
        max_wallets_found=100000
    )

    source.start()

    unique_rows(
        UNIQUE_WALLETS,
        trade_wallets_temp,
        "address",
        output_file=UNIQUE_WALLETS,
        sort_column="timestamp",
        files_to_delete=trade_wallets_temp,
        name="main"
    )

    gd.upload_folder(
        google_drive_folder_id=GdPaths.ADDRESSES,
        local_folder_path=PATHS.ADDRESSES,
        rewrite=True
    )




