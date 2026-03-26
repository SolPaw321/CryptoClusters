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
    valid_files_lengths = []
    extra = f"[{name}] " if name != "" else ""

    main_file = Path(main_file)

    if type(side_files) == list:
        side_files = [Path(f) for f in side_files]
    else:
        side_files = [side_files]

    df_main = pd.read_csv(main_file)
    main_columns = list(df_main.columns)

    valid_dfs = [df_main]
    valid_files_lengths.append(len(df_main))

    for file in side_files:
        df = pd.read_csv(file)

        if list(df.columns) == main_columns:
            valid_dfs.append(df)
            valid_files_lengths.append(len(df))
            print(f"{df.columns=}")
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
