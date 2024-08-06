import os
import pandas as pd
from loguru import logger
from husfort.qutility import SFY
from husfort.qcalendar import CCalendar


class CDbHDF5:
    def __init__(self, db_save_dir: str, db_name: str, table: str):
        """

        :param db_save_dir: directory to save h5 file, like r"E:\\tmp"
        :param db_name: h5 file name, like "test.h5"
        :param table: name for table, structures are allowed, like "grp0/grp1/table"
        """
        self.db_save_dir = db_save_dir
        self.db_name = db_name
        self.table = table

    @property
    def path(self) -> str:
        return os.path.join(self.db_save_dir, self.db_name)

    @property
    def full_name(self) -> str:
        return os.path.join(self.db_save_dir, self.db_name, self.table)

    def has_key(self) -> bool:
        with pd.HDFStore(self.path, mode="a") as store:
            return self.table in store

    def query_all(self) -> pd.DataFrame:
        with pd.HDFStore(self.path, mode="r") as store:
            df: pd.DataFrame = store.get(key=self.table)  # type:ignore
        return df

    def query(self, conds: list[str] | None = None) -> pd.DataFrame:
        """

        :param conds: list of string for conditions, like
                      [
                        "trade_date > '20240101'",
                        "instrument = 'cu'",
                      ]
                      columns to be searched must be labeled as
                      data columns when saving and appending

        :return:
        """
        with pd.HDFStore(self.path, mode="r") as store:
            df: pd.DataFrame = store.select(key=self.table, where=conds)  # type:ignore
        return df

    def head(self, n: int = 5) -> pd.DataFrame:
        with pd.HDFStore(self.path, mode="r") as store:
            df: pd.DataFrame = store.select(key=self.table, start=0, stop=n)  # type:ignore
        return df

    def tail(self, n: int = 5) -> pd.DataFrame:
        with pd.HDFStore(self.path, mode="r") as store:
            df: pd.DataFrame = store.select(key=self.table, start=-n, stop=None)  # type:ignore
        return df

    def put(self, df: pd.DataFrame):
        with pd.HDFStore(self.path, mode="a") as store:
            store.put(key=self.table, value=df, format="fixed", append=False)
        return 0

    def append(self, df: pd.DataFrame, data_columns: list[str] | bool = None):
        with pd.HDFStore(self.path, mode="a") as store:
            store.append(key=self.table, value=df, format="table", append=True, data_columns=data_columns)
        return 0


class CDbHDF5PlusTDates(CDbHDF5):
    """
        This class would assume it has a column named "trade_date", and type = str
        Data in this File must be continuous in the daily scale
    """

    def check_continuity(self, append_date: str, calendar: CCalendar) -> int:
        try:
            rows = self.tail(1)
        except (FileNotFoundError, KeyError):
            logger.warning(
                f"Database {SFY(self.full_name)} may not exist, "
                f"when appending new data, continuity check would assume it is continuous")
            return 0
        if len(rows) > 0:
            expected_next_date = calendar.get_next_date(last_date := rows["trade_date"].iloc[0], shift=1)
        else:
            logger.warning(
                f"Database {SFY(self.full_name)} does exist, but it is empty, "
                f"when appending new data, continuity check would assume it is continuous")
            return 0

        if expected_next_date == append_date:
            return 0
        elif expected_next_date < append_date:
            logger.warning(f"Warning! Last date of {SFY(self.full_name)} is {SFY(last_date)}")
            logger.warning(f"And expected next date should be {SFY(expected_next_date)}")
            logger.warning(f"But date to append = {SFY(append_date)}")
            logger.warning(f"Some days may be {SFY('omitted')}")
            return 1
        else:  # expected_next_date > append_date
            logger.warning(f"Warning! Last date of {SFY(self.full_name)} is {SFY(last_date)}")
            logger.warning(f"And expected next date should be {SFY(expected_next_date)}")
            logger.warning(f"But date to append = {SFY(append_date)}.")
            logger.warning(f"Some days may be {SFY('overlapped')}")
            return 2
