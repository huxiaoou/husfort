import os
import dataclasses
import pandas as pd
import sqlite3 as sql3
from loguru import logger
from husfort.qcalendar import CCalendar, CSection, CCalendarSection
from husfort.qutility import SFR, SFY, SFG


@dataclasses.dataclass(frozen=True)
class CSqlVar:
    name: str
    dtype: str  # ("TEXT", "INTEGER", "REAL")


@dataclasses.dataclass(frozen=True)
class CSqlVars:
    primary_keys: list[CSqlVar]
    value_columns: list[CSqlVar]

    @property
    def size(self) -> int:
        return len(self.primary_keys) + len(self.value_columns)

    @property
    def primary_names(self) -> list[str]:
        return [z.name for z in self.primary_keys]

    @property
    def values_names(self) -> list[str]:
        return [z.name for z in self.value_columns]

    @property
    def names(self) -> list[str]:
        return self.primary_names + self.values_names


class CSqlTable(object):
    def __init__(
            self,
            name: str = None, primary_keys: list[CSqlVar] = None, value_columns: list[CSqlVar] = None,
            cfg: dict = None
    ):
        """

        :param name:
        :param primary_keys:
        :param value_columns:
        :param cfg: use this dict only or use the three specific arguments above together
        """

        if cfg:
            self.name = cfg["name"]
            self.vars: CSqlVars = CSqlVars(
                primary_keys=[CSqlVar(k, v) for k, v in cfg["primary_keys"].items()],
                value_columns=[CSqlVar(k, v) for k, v in cfg["value_columns"].items()],
            )
        else:
            self.name = name
            self.vars: CSqlVars = CSqlVars(primary_keys=primary_keys, value_columns=value_columns)

    def __repr__(self) -> str:
        return (
            "CSqlTable(\n"
            f"name={self.name}\n"
            f"primary_keys={self.vars.primary_keys}\n"
            f"value_columns={self.vars.value_columns}\n)"
        )

    @property
    def cmd_sql_upd(self) -> str:
        str_columns = ", ".join(self.vars.names)
        str_args = ", ".join(["?"] * self.vars.size)
        cmd_upd = f"INSERT OR REPLACE INTO {self.name} ({str_columns}) values({str_args})"
        return cmd_upd

    @property
    def cmd_sql_vars(self) -> str:
        str_primary_keys = [f"{z.name} {z.dtype}" for z in self.vars.primary_keys]
        str_value_columns = [f"{z.name} {z.dtype}" for z in self.vars.value_columns]
        str_all_columns = ", ".join(str_primary_keys + str_value_columns)
        return str_all_columns

    @property
    def cmd_sql_primary(self) -> str:
        str_set_primary = f"PRIMARY KEY({', '.join(self.vars.primary_names)})"
        return str_set_primary


@dataclasses.dataclass(frozen=True)
class CDbStruct:
    db_save_dir: str
    db_name: str
    table: CSqlTable

    def copy_to_another(self, another_db_save_dir: str, another_db_name: str) -> "CDbStruct":
        return CDbStruct(
            db_save_dir=another_db_save_dir,
            db_name=another_db_name,
            table=self.table
        )


class CMgrSqlDb(object):
    def __init__(self, db_save_dir: str, db_name: str, table: CSqlTable, mode: str, verbose: bool = False):
        """

        :param db_save_dir:
        :param db_name:
        :param table:
        :param mode: must be of ('w', 'a', 'r')
        """
        self.db_save_dir: str = db_save_dir
        self.db_name: str = db_name
        self.table: CSqlTable = table
        if mode in ("w", "a", "r"):
            self.mode = mode
            self.__init_table(verbose)
        else:
            raise ValueError(f"mode = {mode} is illegal, options should from =('w', 'a', 'r') ")

    def __init_table(self, verbose: bool):
        """

        :param verbose: if to print more details
        :return:
        """

        # remove old table
        if self.has_table(self.table):
            if verbose:
                logger.info(f"Table {SFG(self.full_table_name)} exists already")

            if self.mode == "w":
                self.remove_table(self.table)
                if verbose:
                    logger.info(f"Table {SFG(self.full_table_name)} is removed, with mode = {SFY(self.mode)}")

        if self.mode in ("w", "a"):
            cmd_sql_for_create_table = (
                f"CREATE TABLE IF NOT EXISTS "
                f"{self.table.name}({self.table.cmd_sql_vars}, "
                f"{self.table.cmd_sql_primary})"
            )
            self.__execute_cmd_write(cmd_sql_for_create_table)
            if verbose:
                logger.info(f"Table {SFG(self.full_table_name)} is initialized")
        return 0

    @property
    def db_path(self) -> str:
        return os.path.join(self.db_save_dir, self.db_name)

    @property
    def full_table_name(self) -> str:
        return f"{self.db_name}/{self.table.name}"

    @staticmethod
    def parse_value_columns(value_columns: list[str] | None) -> str:
        return ", ".join(value_columns) if value_columns else "*"

    def get_column_names(self, value_columns: list[str] | None) -> list[str]:
        return value_columns or self.table.vars.names

    def __execute_cmd_read(self, cmd_sql: str) -> list:
        with sql3.connect(self.db_path) as connection:
            cursor = connection.cursor()
            data = cursor.execute(cmd_sql).fetchall()
        return data

    def check_permission(self) -> bool:
        if self.mode == "r":
            raise ValueError(f"Writing to database is not permitted, with mode = {SFY('r')}")
        return True

    def __execute_cmd_write(self, cmd_sql: str):
        if self.check_permission():
            with sql3.connect(self.db_path) as connection:
                cursor = connection.cursor()
                cursor.execute(cmd_sql)
                connection.commit()
        return 0

    def has_table(self, table: CSqlTable) -> bool:
        cmd_sql_has_table = f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{table.name}'"
        table_counts = self.__execute_cmd_read(cmd_sql_has_table)[0][0]
        return table_counts > 0

    def remove_table(self, table: CSqlTable):
        cmd_sql_rm_table = f"DROP TABLE {table.name}"
        self.__execute_cmd_write(cmd_sql_rm_table)
        return 0

    def read(self, value_columns: list[str] | None = None) -> pd.DataFrame:
        str_value_columns = self.parse_value_columns(value_columns)
        cmd_sql_for_inquiry = f"SELECT {str_value_columns} FROM {self.table.name}"
        rows = self.__execute_cmd_read(cmd_sql_for_inquiry)
        return pd.DataFrame(data=rows, columns=self.get_column_names(value_columns))

    def head(self, n: int = 5, value_columns: list[str] | None = None) -> pd.DataFrame:
        str_value_columns = self.parse_value_columns(value_columns)
        cmd_sql_get_head = f"SELECT {str_value_columns} FROM {self.table.name} ORDER BY rowid LIMIT {n}"
        rows = self.__execute_cmd_read(cmd_sql_get_head)
        return pd.DataFrame(data=rows, columns=self.get_column_names(value_columns))

    def tail(self, n: int = 5, value_columns: list[str] | None = None) -> pd.DataFrame:
        str_value_columns = self.parse_value_columns(value_columns)
        cmd_sql_get_tail = f"SELECT {str_value_columns} FROM {self.table.name} ORDER BY rowid DESC LIMIT {n}"
        rows = self.__execute_cmd_read(cmd_sql_get_tail)[::-1]
        return pd.DataFrame(data=rows, columns=self.get_column_names(value_columns))

    def read_by_conditions(self, conditions: list[tuple[str, str, str]],
                           value_columns: list[str] | None = None) -> pd.DataFrame:
        """

        :param conditions: a list of tuple[str ...], like:
                            [
                                ("instrument", "=", "IC.CFE"),
                                ("tid", "=", "T01"),
                                ("trade_date", ">=", "20120101"),
                                ("trade_date", "<", "20120101"),
                            ],
                            each tuple stands for a condition, the final result of this function
                            is the intersection of these functions
        :param value_columns:
        :return:
        """

        str_value_columns = self.parse_value_columns(value_columns)
        conds_str = " and ".join([f"{c0} {c1} '{c2}'" for c0, c1, c2 in conditions])
        cmd_sql_query = f"SELECT {str_value_columns} FROM {self.table.name} WHERE {conds_str}"
        rows = self.__execute_cmd_read(cmd_sql_query)
        return pd.DataFrame(data=rows, columns=self.get_column_names(value_columns))

    def read_by_date(self, date: str, value_columns: list[str] | None = None):
        return self.read_by_conditions(
            conditions=[("trade_date", "=", date)],
            value_columns=value_columns,
        )

    def read_by_range(self, bgn_date: str, stp_date: str, value_columns: list[str] | None = None):
        return self.read_by_conditions(
            conditions=[
                ("trade_date", ">=", bgn_date),
                ("trade_date", "<", stp_date)
            ],
            value_columns=value_columns,
        )

    def read_by_instrument(self, instrument: str, value_columns: list[str] | None = None):
        return self.read_by_conditions(
            conditions=[("instrument", "=", instrument)],
            value_columns=value_columns,
        )

    def read_by_instrument_range(self, bgn_date: str, stp_date: str,
                                 instrument: str, value_columns: list[str] | None = None):
        return self.read_by_conditions(
            conditions=[
                ("trade_date", ">=", bgn_date),
                ("trade_date", "<", stp_date),
                ("instrument", "=", instrument)
            ],
            value_columns=value_columns,
        )

    def check_continuity(self, incoming_date: str, calendar: CCalendar, check_var: str = "trade_date") -> int:
        tail_data = self.tail(n=1, value_columns=[check_var])
        if tail_data.empty:
            return 0

        expected_next_date = calendar.get_next_date(last_date := tail_data[check_var].iloc[-1], shift=1)
        if expected_next_date == incoming_date:
            return 0
        elif expected_next_date < incoming_date:
            logger.info(f"Warning! Last date of {SFR(self.full_table_name)} is {SFR(last_date)}")
            logger.info(f"And expected next date should be {SFR(expected_next_date)}")
            logger.info(f"But input date = {SFR(incoming_date)}")
            logger.info(f"Some days may be {SFR('omitted')}")
            return 1
        else:  # expected_next_date > append_date
            logger.info(f"Warning! Last date of {SFY(self.full_table_name)} is {SFY(last_date)}")
            logger.info(f"And expected next date should be {SFY(expected_next_date)}")
            logger.info(f"But input date = {SFY(incoming_date)}.")
            logger.info(f"Some days may be {SFY('overlapped')}")
            return 2

    def check_section_continuity(self, append_sec: CSection, calendar: CCalendarSection,
                                 check_vars: tuple[str, str] = ("trade_date", "section")) -> int:
        tail_data = self.tail(n=1, value_columns=list(check_vars))
        if tail_data.empty:
            return 0

        last_date, last_section = tail_data[check_vars[0]].iloc[-1], tail_data[check_vars[1]].iloc[-1]
        tgt_sec_id = f"{last_date}-{last_section}"
        match_res, last_sec = calendar.match_id(tgt_sec_id)
        if match_res:
            expected_next_sec = calendar.get_next_sec(last_sec, 1)
            if expected_next_sec == append_sec:
                return 0
            elif expected_next_sec < append_sec:
                logger.info(f"Expected next section should be {SFR(expected_next_sec.secId)}")
                logger.info(f"But input section = {append_sec.secId}.")
                logger.info(f"Some sections may be {SFY('omitted')}")
                return 1
            else:  # expected_next_sec > append_sec:
                logger.info(f"Expected next section should be {SFR(expected_next_sec.secId)}")
                logger.info(f"But input section = {append_sec.secId}.")
                logger.info(f"Some sections may be {SFY('overlapped')}")
                return 2
        else:
            logger.info(f"{SFY(tgt_sec_id)} is not a right trade section")
            return 3

    def update(self, update_data: pd.DataFrame, using_index: bool = False):
        """

        :param update_data: new data, column orders must be the same as the columns orders of the new target table
        :param using_index: whether using index as a data column
        :return:
        """

        if self.check_permission():
            cmd_upd = self.table.cmd_sql_upd
            with sql3.connect(self.db_path) as connection:
                cursor = connection.cursor()
                for data_cell in update_data.itertuples(index=using_index):  # itertuples is much faster than iterrows
                    cursor.execute(cmd_upd, data_cell)
                connection.commit()
        return 0

    def delete_by_conditions(self, conditions: list[tuple[str, str, str]]):
        """

        :param conditions: a list of tuple, like:[
                                ("instrument", "=", "IC.CFE"),
                                ("tid", "=", "T01"),
                                ("trade_date", ">=", "20120101"),
                                ("trade_date", "<", "20120101"),
                            ],
        :return:
        """
        if self.check_permission():
            conds_str = " AND ".join([f"{c0} {c1} '{c2}'" for c0, c1, c2 in conditions])
            cmd_sql_delete = f"DELETE from {self.table.name} WHERE ({conds_str})"
            self.__execute_cmd_write(cmd_sql_delete)
        return 0

    def delete_by_date(self, trade_date: str):
        self.delete_by_conditions(conditions=[("trade_date", "=", trade_date)])
        return 0

    def delete_by_section(self, section: CSection):
        self.delete_by_conditions(
            conditions=[
                ("trade_date", "=", section.trade_date),
                ("section", "=", section.section)],
        )
        return 0
