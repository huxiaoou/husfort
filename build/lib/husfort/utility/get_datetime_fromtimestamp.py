import datetime as dt
import argparse


def get_datetime_fromtimestamp(timestamp):
    t = dt.datetime.fromtimestamp(timestamp)
    print(t)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-t", "--timestamp", required=True, type=int)
    args = arg_parser.parse_args()
    get_datetime_fromtimestamp(args.timestamp)
