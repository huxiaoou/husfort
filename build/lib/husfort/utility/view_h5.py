if __name__ == "__main__":
    import sys
    import argparse
    import pandas as pd

    args_parser = argparse.ArgumentParser(description="A python script to view hdf5")
    args_parser.add_argument("--lib", type=str, required=True,
                             help="path for h5 file, like 'E:\\tmp\\test.h5'")
    args_parser.add_argument("--table", type=str, required=True,
                             help="table name in the h5 file, like '/grp1/grp2/testTable'")
    args_parser.add_argument("--head", type=int, default=0, help="integer, head lines to print")
    args_parser.add_argument("--tail", type=int, default=0, help="integer, tail lines to print")
    args_parser.add_argument("--conds", type=str, default=None,
                             help=
                             "conds to filter, multiple conds are separated by ';', "
                             "like \"instrument = 'a' | instrument = 'd';trade_date <= '20120131'\" "
                             )
    args = args_parser.parse_args()

    with pd.HDFStore(path=args.lib, mode="r") as store:
        if args.conds:
            df: pd.DataFrame = store.select(key=args.table, where=args.conds.split(";"))  # type:ignore
        else:
            df: pd.DataFrame = store.get(key=args.table)  # type:ignore

    if args.head > 0:
        print(df.head(args.head))
        sys.exit()
    if args.tail > 0:
        print(df.tail(args.tail))
        sys.exit()

    print(df)
