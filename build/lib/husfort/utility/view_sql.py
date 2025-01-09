import argparse
import sqlite3 as sql3
import pandas as pd


def parse_args():
    args_parser = argparse.ArgumentParser(description="A python script to view hdf5")
    args_parser.add_argument(
        "--lib",
        type=str,
        required=True,
        help="path for sql file, like 'E:\\tmp\\alternative.db'",
    )
    args_parser.add_argument(
        "--table",
        type=str,
        required=True,
        help="table name in the sql file, like 'macro' or 'forex' in alternative.db",
    )
    args_parser.add_argument(
        "--vars",
        type=str,
        default=None,
        help="variables to fetch, separated by ',' like \"open,high,low,close\", "
        "if not provided then fetch all.",
    )
    args_parser.add_argument(
        "--where",
        type=str,
        default=None,
        help="conditions to filter, sql expression "
        "like \"(instrument = 'a' OR instrument = 'd') AND (trade_date <= '20120131')\" ",
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
    _args = args_parser.parse_args()
    return _args


def get_table_names(lib: str, table: str) -> list[str]:
    with sql3.connect(lib) as connection:
        cursor = connection.cursor()
        sql = f"select * from {table} where 1=0;"
        cursor.execute(sql)
        _names = [d[0] for d in cursor.description]
    return _names


def fetch(lib: str, table: str, names: list[str], conds: str) -> pd.DataFrame:
    var_str = ",".join(names)
    with sql3.connect(lib) as connection:
        cursor = connection.cursor()
        cmd_sql = f"SELECT {var_str} from {table} {f'WHERE {conds}' if conds else ''}"
        data = cursor.execute(cmd_sql).fetchall()
        _df = pd.DataFrame(data, columns=names)
    return _df


if __name__ == "__main__":
    import sys

    pd.set_option("display.unicode.east_asian_width", True)

    args = parse_args()
    if args.maxrows > 0:
        pd.set_option("display.max_rows", args.maxrows)
    if args.maxcols > 0:
        pd.set_option("display.max_columns", args.maxcols)

    col_names = (
        args.vars.split(",") if args.vars else get_table_names(args.lib, args.table)
    )
    df = fetch(args.lib, args.table, col_names, args.where)
    if args.head > 0:
        print(df.head(args.head))
        sys.exit()
    if args.tail > 0:
        print(df.tail(args.tail))
        sys.exit()
    print(df)
