from src.addresses.finders import TradesSource, TransactionSink
from src.google_drive.GoogleDriveHandler import GoogleDriveHandler
import src.google_drive.folders_ids as gd_ids
import src.paths as loc_path
from src.misc.csv import unique_rows


if __name__ == "__main__":
    UNIQUE_WALLETS_TEMP = loc_path.ADDRESSES_TEMP / "unique_wallets.csv"
    UNIQUE_WALLETS = loc_path.ADDRESSES / "unique_wallets.csv"

    gd = GoogleDriveHandler()
    if not UNIQUE_WALLETS.exists():
        gd.download_folder(
            google_drive_folder_id=gd_ids.ADDRESSES_FOLDER_ID,
            local_folder_path=loc_path.ADDRESSES,
            rewrite=False
        )
    gd.download_folder(
        google_drive_folder_id=gd_ids.ADDRESSES_FOLDER_ID,
        local_folder_path=loc_path.ADDRESSES_TEMP,
        rewrite=True
    )

    unique_rows(
        UNIQUE_WALLETS,
        UNIQUE_WALLETS_TEMP,
        "address",
        output_file=UNIQUE_WALLETS,
        files_to_delete=UNIQUE_WALLETS_TEMP
    )


    trade_wallets_temp = loc_path.ADDRESSES_TEMP / "trade_wallets_temp.csv"

    sink = TransactionSink(csv_path=trade_wallets_temp, batch_size=1000)

    source = TradesSource(
        sink=sink,
        markets=["BTC", "ETH", "SOL"],
        max_running_time=6000.0,
        max_wallets_found=10000
    )

    source.start()

    unique_rows(
        UNIQUE_WALLETS,
        trade_wallets_temp,
        "address",
        output_file=UNIQUE_WALLETS,
        sort_column="timestamp",
        files_to_delete=trade_wallets_temp,
    )

    gd.upload_folder(
        google_drive_folder_id=gd_ids.ADDRESSES_FOLDER_ID,
        local_folder_path=loc_path.ADDRESSES,
        rewrite=True
    )




