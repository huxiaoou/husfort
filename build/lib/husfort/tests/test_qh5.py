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
    from husfort.qutility import SFY
    from husfort.qcalendar import CCalendar
    from husfort.qlog import define_logger
    from husfort.qh5 import CDbHDF5PlusTDates

    define_logger()

    calendar_path = r"E:\Deploy\Data\Calendar\cne_calendar.csv"
    calendar = CCalendar(calendar_path)
    h_dates = calendar.get_iter_list(bgn_date="20120101", stp_date="20250101")

    db_save_dir, db_name = r"E:\tmp", "test.h5"
    table_name = "grp1/grp2/testTable"
    nr, nc = 20, 5
    cnms = [f"C{_:02d}" for _ in range(nc)]
    df = create_data(nr * 2, nc, cnames=cnms, hist_dates=h_dates)
    df_head, df_tail = df.head(nr), df.tail(nr)

    h5lib = CDbHDF5PlusTDates(db_save_dir=db_save_dir, db_name=db_name, table=table_name)

    # --- first writing
    append_date = df_head["trade_date"].iloc[0]
    if h5lib.check_continuity(append_date=append_date, calendar=calendar) == 0:
        h5lib.append(df=df_head, data_columns=["trade_date", "instrument"])
        df0 = h5lib.query_all()
        logger.info(f"The original data length = {len(df0)} ")
        print(df0)

    # --- appending
    append_date = df_tail["trade_date"].iloc[0]
    if h5lib.check_continuity(append_date=append_date, calendar=calendar) == 0:
        h5lib.append(df_tail)
        logger.info("Append data to lib")
        df0 = h5lib.query_all()
        logger.info(f"After appending, data length = {len(df0)} ")
        print(df0)

    # --- query
    df1 = h5lib.query(conds=["trade_date <= '20120131'"])
    logger.info("Query: trade_date <= '20120131'")
    print(df1)

    df2 = h5lib.query(conds=["instrument = 'a' | instrument = 'b'"])
    logger.info("Query: instrument = 'a' | instrument = 'b'")
    print(df2)

    df3 = h5lib.query(conds=["instrument = 'd' | instrument = 'c'", "trade_date < '20120205'"])
    logger.info("Query: (instrument = 'd' | instrument = 'c') AND (trade_date < '20120205')")
    print(df3)

    head_data = h5lib.head(10)
    logger.info("10 head lines")
    print(head_data)

    tail_data = h5lib.tail(10)
    logger.info("10 tail lines")
    print(tail_data)

    # --- continuity check
    h5lib.check_continuity(append_date="20120306", calendar=calendar)
    h5lib.check_continuity(append_date="20120307", calendar=calendar)
    h5lib.check_continuity(append_date="20120308", calendar=calendar)

    table_name_alt = "grp1/grp2/testTable2"
    h5lib = CDbHDF5PlusTDates(db_save_dir=db_save_dir, db_name=db_name, table=table_name_alt)
    if h5lib.check_continuity(append_date="20120301", calendar=calendar) == 0:
        logger.info(f"Continuity checking for empty table: {SFY(h5lib.full_name)} is successful.")
