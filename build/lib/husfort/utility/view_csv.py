import argparse
import pandas as pd


def parse_args():
    args_parser = argparse.ArgumentParser(description="A python script to view hdf5")
    args_parser.add_argument("--path", type=str, required=True,
                             help="path for csv file, like 'E:\\tmp\\test.csv.gz' or 'test.csv'")
    args_parser.add_argument("--vars", type=str, default=None,
                             help="variables to fetch, separated by ',' like \"open,high,low,close\", "
                                  "if not provided then fetch all."
                             )
    args_parser.add_argument("--head", type=int, default=0, help="integer, head lines to print")
    args_parser.add_argument("--tail", type=int, default=0, help="integer, tail lines to print")
    args_parser.add_argument("--conds", type=str, default=None,
                             help="conds to filter, accepted by pandas.query "
                                  "like \"(instrument = 'a' | instrument = 'd') & (trade_date <= '20120131')\" "
                             )
    _args = args_parser.parse_args()
    return _args


if __name__ == "__main__":
    import sys

    pd.set_option('display.unicode.east_asian_width', True)
    args = parse_args()
    col_names = args.vars.split(",") if args.vars else []
    df = pd.read_csv(args.path)
    if args.conds:
        df = df.query(args.conds)
    if col_names:
        df = df[col_names]

    if args.head > 0:
        print(df.head(args.head))
        sys.exit()
    if args.tail > 0:
        print(df.tail(args.tail))
        sys.exit()
    print(df)
