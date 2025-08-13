#!/usr/bin/env python

if __name__ == "__main__":
    import argparse
    from husfort.qutility import SFG, SFY
    from husfort.qdataviewer import CDataViewerSql

    args_parser = argparse.ArgumentParser(description="A program to show tables in sqlite database")
    args_parser.add_argument("--lib", required=True, type=str, help="lib path")
    args = args_parser.parse_args()

    data_viewer = CDataViewerSql(lib=args.lib)
    tables = data_viewer.get_tables()
    print(f"There are {SFG(len(tables))} tables in {SFY(args.lib)}")
    for i, table in enumerate(tables):
        print(f"{i:->3d}: {table}")
