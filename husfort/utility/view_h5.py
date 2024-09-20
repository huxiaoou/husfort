import argparse


def parse_args():
    args_parser = argparse.ArgumentParser(description="A python script to view hdf5")
    args_parser.add_argument(
        "--lib",
        type=str,
        required=True,
        help="path for h5 file, like 'E:\\tmp\\test.h5'",
    )
    args_parser.add_argument(
        "--table",
        type=str,
        required=True,
        help="table name in the h5 file, like '/grp1/grp2/testTable'",
    )
    args_parser.add_argument(
        "--head", type=int, default=0, help="integer, head lines to print"
    )
    args_parser.add_argument(
        "--tail", type=int, default=0, help="integer, tail lines to print"
    )
    args_parser.add_argument(
        "--maxrows",
        type=int,
        default=0,
        help="integer, provide larger value to see more rows when print outcomes",
    )
    args_parser.add_argument(
        "--where",
        type=str,
        default=None,
        help="conditions to filter, multiple conditions are separated by ';', "
        "like \"instrument = 'a' | instrument = 'd';trade_date <= '20120131'\" ",
    )
    _args = args_parser.parse_args()
    return _args


if __name__ == "__main__":
    import sys
    import pandas as pd

    pd.set_option("display.unicode.east_asian_width", True)

    args = parse_args()
    if args.maxrows > 0:
        pd.set_option("display.max_rows", args.maxrows)

    with pd.HDFStore(path=args.lib, mode="r") as store:
        if args.where:
            df: pd.DataFrame = store.select(  # type:ignore
                key=args.table, where=args.where.split(";")
            )
        else:
            df: pd.DataFrame = store.get(key=args.table)  # type:ignore

    if args.head > 0:
        print(df.head(args.head))
        sys.exit()
    if args.tail > 0:
        print(df.tail(args.tail))
        sys.exit()

    print(df)
