import os
import pandas as pd


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

    def append(self, df: pd.DataFrame, data_columns: list[str] | bool = None):
        with pd.HDFStore(self.path, mode="a") as store:
            store.append(key=self.table, value=df, format="table", data_columns=data_columns)
        return 0
