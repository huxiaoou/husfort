import pandas as pd
import scipy.stats as sps
import datetime as dt
import random


def create_data(nrow: int, ncol: int, cnames: list[str], start_id: int,
                random_state: int = None) -> pd.DataFrame:
    _data = sps.norm.rvs(size=(nrow, ncol), loc=0, scale=1, random_state=random_state)
    _df = pd.DataFrame(data=_data, columns=cnames)
    _start_date = dt.datetime(year=2012, month=1, day=1)
    _df["trade_date"] = [(_start_date + dt.timedelta(days=_)).strftime("%Y%m%d") for _ in range(nrow)]
    _df["instrument"] = random.choices(population=list("abcd"), k=nrow)
    _df = _df[["trade_date"] + ["instrument"] + cnames]
    return _df


if __name__ == "__main__":
    from loguru import logger
    from husfort.qlog import define_logger
    from husfort.qh5 import CDbHDF5

    define_logger()

    db_save_dir, db_name = r"E:\\tmp", "test.h5"
    table_name = "grp1/grp2/testTable"
    nr, nc = 20, 5
    cnms = [f"C{_:02d}" for _ in range(nc)]
    df = create_data(nr * 2, nc, cnames=cnms, start_id=0)
    df_head, df_tail = df.head(nr), df.tail(nr)

    h5lib = CDbHDF5(db_save_dir=db_save_dir, db_name=db_name, table=table_name)

    # --- first writing
    h5lib.append(df=df_head, data_columns=["trade_date", "instrument"])
    df0 = h5lib.query_all()
    logger.info(f"The original data length = {len(df0)} ")
    print(df0)

    # --- appending
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
