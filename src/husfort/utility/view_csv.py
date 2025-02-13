#!/usr/bin/env python

import argparse
import pandas as pd


def parse_args():
    args_parser = argparse.ArgumentParser(description="A python script to view CSV OR EXCEL files")
    args_parser.add_argument(
        "--path",
        type=str,
        required=True,
        help="path for csv file, like 'E:\\tmp\\test.csv.gz' or 'test.csv'",
    )
    args_parser.add_argument(
        "--vars",
        type=str,
        default=None,
        help="variables to fetch, separated by ',' like \"open,high,low,close\", "
        "if not provided then fetch all.",
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
        "--maxcols",
        type=int,
        default=0,
        help="integer, provide larger value to see more columns when print outcomes",
    )
    args_parser.add_argument(
        "--where",
        type=str,
        default=None,
        help="conditions to filter, accepted by pandas.query "
        "like \"(instrument = 'a' | instrument = 'd') & (trade_date <= '20120131')\" ",
    )
    args_parser.add_argument(
        "--header",
        type=int,
        default=0,
        help="row number of headers, use -1 if there is no header in the source file"
    )
    _args = args_parser.parse_args()
    return _args


if __name__ == "__main__":
    import sys

    pd.set_option("display.unicode.east_asian_width", True)

    args = parse_args()
    if args.maxrows > 0:
        pd.set_option("display.max_rows", args.maxrows)
    if args.maxcols > 0:
        pd.set_option("display.max_columns", args.maxcols)

    col_names = args.vars.split(",") if args.vars else []
    if args.path.endswith(".xls") or args.path.endswith(".xlsx"):
        df = pd.read_excel(args.path, header=args.header if args.header >= 0 else None)
    else:
        df = pd.read_csv(args.path, header=args.header if args.header >= 0 else None)
    if args.where:
        df = df.query(args.where)
    if col_names:
        df = df[col_names]

    if args.head > 0:
        print(df.head(args.head))
        sys.exit()
    if args.tail > 0:
        print(df.tail(args.tail))
        sys.exit()
    print(df)
