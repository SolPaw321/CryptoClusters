from pathlib import Path
import pandas as pd


def unique_rows(
        main_file: str | Path,
        side_files: list[str | Path] | str | Path,
        unique_column: str,
        output_file: str | Path | None = None,
        sort_column: str = None,
        ascending: bool = True,
        name: str = "",
        files_to_delete: list[str | Path] = None) -> None:
    """
    Read csv files, contact, remove duplicates and sort them.

    The :param main_file: gives column order.

    :param main_file: the main file path (gives column order),
    :param side_files: side files paths,
    :param unique_column: to drop duplicates (main file must contain),
    :param output_file: output file path
    :param sort_column: sort by a column
    :param ascending: true or false
    :param name: extra name for print/logging communicates
    :param files_to_delete: files to delete after contact; if the output file is the same as main file, then it will not be deleted
    :return: None
    """
    valid_files_lengths = []
    extra = f"[{name}] " if name != "" else ""

    main_file_path = Path(main_file)
    if not main_file_path.exists():
        raise FileNotFoundError(f"Main file {main_file} not exists.")

    if type(side_files) == list:
        side_file_paths = set()
        for f in side_files:
            file = Path(f)
            if not file.exists():
                print(f"{extra}Side file {file} not exists. Skipping.")
            else:
                side_file_paths.add(file)
    else:
        file = Path(side_files)
        if not file.exists():
            print(f"{extra}Side file {file} not exists. Skipping.")
        side_file_paths = {file}

    if main_file_path in side_file_paths:
        side_file_paths.difference_update({main_file_path})
        if not len(side_file_paths):
            raise ValueError(f"{extra}Lack of side files.")
        print(f"{extra}Main file deleted from side files.")


    df_main = pd.read_csv(main_file)
    main_columns = list(df_main.columns)

    valid_dfs = [df_main]
    valid_files_lengths.append(len(df_main))

    for file in side_file_paths:
        df = pd.read_csv(file)

        if list(df.columns) == main_columns:
            valid_dfs.append(df)
            valid_files_lengths.append(len(df))
        else:
            print(f"{extra}File {file} has not valid columns.")

    result = (
        pd.concat(valid_dfs, ignore_index=True)
        .drop_duplicates(subset=[unique_column], keep="first")
    )

    if sort_column is not None:
        result = result.sort_values(by=sort_column, ascending=ascending)

    if output_file is None:
        output_file = main_file

    result.to_csv(output_file, index=False)
    print(f"{extra}Saved to: {output_file}")

    if files_to_delete is not None:
        if type(files_to_delete) != list:
            files_to_delete = [files_to_delete]

        for f in files_to_delete:
            f = Path(f)
            if f != main_file:
                f.unlink(missing_ok=True)
                print(f"{extra}File {f} deleted.")
