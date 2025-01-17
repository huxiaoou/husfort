import sqlite3

import pandas as pd
import scipy.stats as sps
import random


def create_data(nrow: int, ncol: int, cnames: list[str], hist_dates: list[str],
                random_state: int = None) -> pd.DataFrame:
    _data = sps.norm.rvs(size=(nrow, ncol), loc=0, scale=1, random_state=random_state)
    _df = pd.DataFrame(data=_data, columns=cnames)
    _df["trade_date"] = hist_dates[0:nrow]
    _df["instrument"] = random.choices(population=list("abcd"), k=nrow)
    _df = _df[["trade_date"] + ["instrument"] + cnames]
    return _df


if __name__ == "__main__":
    from loguru import logger
    from src.husfort.qutility import SFY
    from src.husfort.qcalendar import CCalendar
    from src.husfort.qlog import define_logger
    from src.husfort.qsqlite import CMgrSqlDb, CSqlTable, CSqlVar

    define_logger()

    calendar_path = r"E:\Deploy\Data\Calendar\cne_calendar.csv"
    calendar = CCalendar(calendar_path)
    h_dates = calendar.get_iter_list(bgn_date="20120101", stp_date="20250101")

    db_save_dir, db_name = r"E:\tmp", "test.db"
    table_name = "testTable"
    nr, nc = 20, 5
    cnms = [f"C{_:02d}" for _ in range(nc)]
    df = create_data(nr * 2, nc, cnames=cnms, hist_dates=h_dates)
    df_head, df_tail = df.head(nr), df.tail(nr).reset_index(drop=True)
    table = CSqlTable(
        name=table_name,
        primary_keys=[CSqlVar("trade_date", "TEXT"), CSqlVar("instrument", "TEXT")],
        value_columns=[CSqlVar(_, "REAL") for _ in cnms]
    )
    sql_lib = CMgrSqlDb(
        db_save_dir=db_save_dir,
        db_name=db_name,
        table=table,
        mode="w",
        verbose=True,
    )

    # --- first writing
    append_date = df_head["trade_date"].iloc[0]
    if sql_lib.check_continuity(incoming_date=append_date, calendar=calendar) == 0:
        sql_lib.update(df_head)
        df0 = sql_lib.read()
        logger.info(f"The original data length = {len(df0)} ")
        print(df0)

    # --- appending
    append_date = df_tail["trade_date"].iloc[0]
    if sql_lib.check_continuity(incoming_date=append_date, calendar=calendar) == 0:
        sql_lib.update(df_tail)
        logger.info("Append data to lib")
        df0 = sql_lib.read()
        logger.info(f"After appending, data length = {len(df0)} ")
        print(df0)

    # --- head and tail
    logger.info("The first 5 rows from the data, with columns=['trade_date', 'C00']")
    print(sql_lib.head(value_columns=["trade_date", "C00"]))
    logger.info("The last 10 rows from the data")
    print(sql_lib.tail(n=10))

    # --- query
    df1 = sql_lib.read_by_conditions(conditions=[("trade_date", "<=", "20120131")])
    logger.info("Query: trade_date <= '20120131'")
    print(df1)

    df3 = sql_lib.read_by_conditions(conditions=[("instrument", "=", "d"), ("trade_date", "<", "20120205")])
    logger.info("Query: (instrument = 'd') AND (trade_date < '20120205')")
    print(df3)

    # --- continuity check
    sql_lib.check_continuity(incoming_date="20120306", calendar=calendar)
    sql_lib.check_continuity(incoming_date="20120307", calendar=calendar)
    sql_lib.check_continuity(incoming_date="20120308", calendar=calendar)

    table_name2 = "testTable2"
    table2 = CSqlTable(
        name=table_name2,
        primary_keys=[CSqlVar("trade_date", "TEXT"), CSqlVar("instrument", "TEXT")],
        value_columns=[CSqlVar(_, "REAL") for _ in cnms]
    )
    sql_lib = CMgrSqlDb(
        db_save_dir=db_save_dir,
        db_name=db_name,
        table=table2,
        mode="a",
        verbose=True,
    )
    if sql_lib.check_continuity(incoming_date="20240807", calendar=calendar) == 0:
        logger.info(f"Continuity checking for empty table: {SFY(sql_lib.full_table_name)} is successful.")

    table_name3 = "testTable3"
    table3 = CSqlTable(
        name=table_name3,
        primary_keys=[CSqlVar("trade_date", "TEXT"), CSqlVar("instrument", "TEXT")],
        value_columns=[CSqlVar(_, "REAL") for _ in cnms]
    )
    sql_lib = CMgrSqlDb(
        db_save_dir=db_save_dir,
        db_name=db_name,
        table=table3,
        mode="r",
        verbose=True,
    )
    try:
        sql_lib.check_continuity(incoming_date="20240807", calendar=calendar)
    except sqlite3.OperationalError as e:
        logger.exception(e)
