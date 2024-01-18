import os
import pandas as pd
import sqlite3 as sql3
from husfort.qcalendar import CCalendar, CSection, CCalendarSection
from husfort.qutility import SFR, SFY


class CTable(object):
    def __init__(self, table_struct: dict):
        """

        :param table_struct: should always contain at least three key-value pairs
                0. table_name: str
                1. primary_keys: {"key_name": "key_datatype"}
                2. value_columns: {"value_name": "value_datatype"}
        """

        self.table_name: str = table_struct["table_name"]
        self.primary_keys: dict[str, str] = table_struct["primary_keys"]
        self.value_columns: dict[str, str] = table_struct["value_columns"]

        self.vars: list[str] = list(self.primary_keys.keys()) + list(self.value_columns.keys())
        self.vars_n = len(self.vars)

        # cmd for update
        str_columns = ", ".join(self.vars)
        str_args = ", ".join(["?"] * self.vars_n)
        self.m_cmd_sql_update_template = f"INSERT OR REPLACE INTO {self.table_name} ({str_columns}) values({str_args})"


class CMangerLibBase(object):
    def __init__(self, db_save_dir: str, db_name: str):
        self.db_save_dir: str = db_save_dir
        self.db_name: str = db_name
        self.db_path: str = os.path.join(db_save_dir, db_name)
        self.connection = sql3.connect(self.db_path)
        self.cursor = self.connection.cursor()
        self.manager_table: dict[str, CTable] = {}
        self.default_table_name: str = ""

    def commit(self):
        self.connection.commit()
        return 0

    def close(self):
        self.cursor.close()
        self.connection.close()
        return 0

    def is_table_existence(self, table: CTable) -> bool:
        cmd_sql_check_existence = (f"SELECT count(name) FROM sqlite_master"
                                   f" WHERE type='table' AND name='{table.table_name}'")
        table_counts = self.cursor.execute(cmd_sql_check_existence).fetchall()[0][0]
        return table_counts > 0

    def reconnect(self):
        self.connection = sql3.connect(self.db_path)
        self.cursor = self.connection.cursor()
        return 0

    def get_table(self, table_name: str) -> CTable:
        return self.manager_table[table_name]


class CManagerLibReader(CMangerLibBase):
    def set_default(self, default_table_name: str):
        self.default_table_name = default_table_name
        return 0

    def choose_table(self, using_default_table: bool = True, table_name: str = ""):
        return self.default_table_name if using_default_table else table_name

    def read(self, value_columns: list[str], using_default_table: bool = True, table_name: str = "") -> pd.DataFrame:
        tgt_table_name = self.choose_table(using_default_table, table_name)
        str_value_columns = ", ".join(value_columns)
        cmd_sql_for_inquiry = f"SELECT {str_value_columns} FROM {tgt_table_name}"
        rows = self.cursor.execute(cmd_sql_for_inquiry).fetchall()
        return pd.DataFrame(data=rows, columns=value_columns)

    def read_by_conditions(self, conditions: list[tuple[str, str, str]], value_columns: list[str],
                           using_default_table: bool = True, table_name: str = "") -> pd.DataFrame:
        """

        :param conditions: a list of tuple, like:[
                            ("instrument", "=", "IC.CFE"),
                            ("tid", "=", "T01"),
                            ("trade_date", ">=", "20120101"),
                            ("trade_date", "<", "20120101"),
                            ],
        :param value_columns:
        :param using_default_table:
        :param table_name:
        :return:
        """
        tgt_table_name = self.choose_table(using_default_table, table_name)
        str_value_columns = ", ".join(value_columns)
        conds_str = " and ".join([f"{c0} {c1} '{c2}'" for c0, c1, c2 in conditions])
        cmd_sql_for_inquiry = f"SELECT {str_value_columns} FROM {tgt_table_name} where {conds_str}"
        rows = self.cursor.execute(cmd_sql_for_inquiry).fetchall()
        return pd.DataFrame(data=rows, columns=value_columns)

    def read_by_date(self, date: str, value_columns: list[str], using_default_table: bool = True, table_name: str = ""):
        return self.read_by_conditions(conditions=[("trade_date", "=", date)],
                                       value_columns=value_columns, using_default_table=using_default_table,
                                       table_name=table_name)

    def read_by_range(self, bgn_date: str, stp_date: str, value_columns: list[str], using_default_table: bool = True,
                      table_name: str = ""):
        return self.read_by_conditions(conditions=[("trade_date", ">=", bgn_date), ("trade_date", "<", stp_date)],
                                       value_columns=value_columns, using_default_table=using_default_table,
                                       table_name=table_name)

    def read_by_instrument(self, instrument: str, value_columns: list[str], using_default_table: bool = True,
                           table_name: str = ""):
        return self.read_by_conditions(conditions=[("instrument", "=", instrument)],
                                       value_columns=value_columns, using_default_table=using_default_table,
                                       table_name=table_name)

    def read_by_instrument_range(self, bgn_date: str, stp_date: str, instrument: str,
                                 value_columns: list[str], using_default_table: bool = True, table_name: str = ""):
        return self.read_by_conditions(
            conditions=[("trade_date", ">=", bgn_date), ("trade_date", "<", stp_date), ("instrument", "=", instrument)],
            value_columns=value_columns, using_default_table=using_default_table, table_name=table_name)

    def check_continuity(self, append_date: str, calendar: CCalendar,
                         using_default_table: bool = True, table_name: str = "") -> int:
        tgt_table_name = self.choose_table(using_default_table, table_name)
        cmd_sql_get_last_date = f"SELECT trade_date FROM {tgt_table_name} ORDER BY rowid DESC LIMIT 1;"
        rows = self.cursor.execute(cmd_sql_get_last_date).fetchall()
        if len(rows) > 0:
            expected_next_date = calendar.get_next_date(last_date := rows[0][0], 1)
        else:
            last_date, expected_next_date = "not available", ""

        if expected_next_date == append_date:
            return 0
        elif expected_next_date < append_date:
            print(f"... [INF] Warning! Last date of {SFR(tgt_table_name)} is {last_date}")
            print(f"... [INF] And expected next date should be {SFR(expected_next_date)}")
            print(f"... [INF] But input date = {append_date}")
            print(f"... [INF] Some days may be {SFY('omitted')}")
            return 1
        else:  # expected_next_date > append_date
            print(f"... [INF] Warning! Last date of {SFR(tgt_table_name)} is {last_date}")
            print(f"... [INF] And expected next date should be {SFR(expected_next_date)}")
            print(f"... [INF] But input date = {append_date}.")
            print(f"... [INF] Some days may be {SFY('overlapped')}")
            return 2

    def check_section_continuity(self, append_sec: CSection, calendar: CCalendarSection,
                                 using_default_table: bool = True, table_name: str = "") -> int:
        tgt_table_name = self.choose_table(using_default_table, table_name)
        cmd_sql_get_last_date = f"SELECT trade_date, section FROM {tgt_table_name} ORDER BY rowid DESC LIMIT 1;"
        rows = self.cursor.execute(cmd_sql_get_last_date).fetchall()
        if len(rows) > 0:
            last_date, last_section = rows[0]
            tgt_sec_id = f"{last_date}-{last_section}"
            match_res, last_sec = calendar.match_id(tgt_sec_id)
            if match_res:
                expected_next_sec = calendar.get_next_sec(last_sec, 1)
            else:
                print(f"... [INF] {SFY(tgt_sec_id)} is not a right trade section")
                expected_next_sec = None
        else:
            print(f"... [INF] No last section available in {tgt_table_name}")
            expected_next_sec = None

        if expected_next_sec is None:
            return 3
        if expected_next_sec == append_sec:
            return 0
        elif expected_next_sec < append_sec:
            print(f"... [INF] Expected next section should be {SFR(expected_next_sec.secId)}")
            print(f"... [INF] But input section = {append_sec.secId}.")
            print(f"... [INF] Some sections may be {SFY('omitted')}")
            return 1
        else:  # expected_next_sec > append_sec:
            print(f"... [INF] Expected next section should be {SFR(expected_next_sec.secId)}")
            print(f"... [INF] But input section = {append_sec.secId}.")
            print(f"... [INF] Some sections may be {SFY('overlapped')}")
            return 2


class CManagerLibWriter(CManagerLibReader):
    def remove_table(self, table_name: str):
        self.cursor.execute(f"DROP TABLE {table_name}")
        return 0

    def initialize_table(self, table: CTable, remove_existence: bool = True, set_as_default: bool = True,
                         verbose: bool = False):
        """

        :param table:
        :param remove_existence: if to remove the existing table if it already has one
        :param set_as_default: if set this table as default table
        :param verbose: if to print more details
        :return:
        """
        self.manager_table[table.table_name] = table

        # remove old table
        if self.is_table_existence(table):
            if verbose:
                print(f"... Table {table.table_name} is in database {self.db_name} already")
            if remove_existence:
                self.remove_table(table.table_name)
                if verbose:
                    print(f"... Table {table.table_name} is removed from database {self.db_name}")

        str_primary_keys = [f"{k} {v}" for k, v in table.primary_keys.items()]
        str_value_columns = [f"{k} {v}" for k, v in table.value_columns.items()]
        str_all_columns = ", ".join(str_primary_keys + str_value_columns)
        str_set_primary = f"PRIMARY KEY({', '.join(table.primary_keys)})"
        cmd_sql_for_create_table = (f"CREATE TABLE IF NOT EXISTS "
                                    f"{table.table_name}({str_all_columns}, {str_set_primary})")
        self.cursor.execute(cmd_sql_for_create_table)
        if set_as_default:
            self.set_default(default_table_name=table.table_name)
        if verbose:
            print(f"... Table {table.table_name} in {self.db_name} is initialized")
        return 0

    def initialize_tables(self, tables: list[CTable], remove_existence: bool = True, default_table_name: str = "",
                          verbose: bool = False):
        for table in tables:
            self.initialize_table(table, remove_existence, table.table_name == default_table_name, verbose)
        return 0

    def update(self, update_df: pd.DataFrame, using_index: bool = False, using_default_table: bool = True,
               table_name: str = ""):
        """

        :param update_df: new data, column orders must be the same as the columns orders of the new target table
        :param using_index: whether using index as a data column
        :param using_default_table: whether using the default table
        :param table_name: the table to be updated, if t_using_default_table is False
        :return:
        """
        tgt_table_name = self.choose_table(using_default_table, table_name)
        cmd_sql_update = self.manager_table[tgt_table_name].m_cmd_sql_update_template
        for data_cell in update_df.itertuples(index=using_index):  # itertuples is much faster than iterrows
            self.cursor.execute(cmd_sql_update, data_cell)
        return 0

    def delete_by_conditions(self, conditions: list[tuple[str, str, str]], using_default_table: bool = True,
                             table_name: str = ""):
        """

        :param conditions: a list of tuple, like:[
                            ("instrument", "=", "IC.CFE"),
                            ("tid", "=", "T01"),
                            ("trade_date", ">=", "20120101"),
                            ("trade_date", "<", "20120101"),
                            ],
        :param using_default_table:
        :param table_name:
        :return:
        """
        tgt_table_name = self.choose_table(using_default_table, table_name)
        conds_str = " and ".join([f"{c0} {c1} '{c2}'" for c0, c1, c2 in conditions])
        cmd_sql_delete = f"DELETE from {tgt_table_name} where ({conds_str})"
        self.cursor.execute(cmd_sql_delete)
        return 0

    def delete_by_date(self, date: str, using_default_table: bool = True, table_name: str = ""):
        self.delete_by_conditions(conditions=[("trade_date", "=", date)], using_default_table=using_default_table,
                                  table_name=table_name)
        return 0

    def delete_by_section(self, section: CSection, using_default_table: bool = True, table_name: str = ""):
        self.delete_by_conditions(
            conditions=[("trade_date", "=", section.trade_date), ("section", "=", section.section)],
            using_default_table=using_default_table, table_name=table_name)
        return 0


class CLib1Tab1(object):
    def __init__(self, lib_name: str, table: CTable):
        self.lib_name: str = lib_name
        self.table: CTable = table


class CQuickSqliteLib(object):
    def __init__(self, lib_name: str, lib_save_dir: str):
        self.lib_name = lib_name
        self.lib_save_dir = lib_save_dir

    def get_lib_struct(self) -> CLib1Tab1:
        pass

    def get_lib_writer(self, run_mode: str) -> CManagerLibWriter:
        lib_struct = self.get_lib_struct()
        lib_writer = CManagerLibWriter(self.lib_save_dir, lib_struct.lib_name)
        if run_mode.lower() in ["o", "overwrite"]:
            lib_writer.initialize_table(lib_struct.table, remove_existence=True)
        elif run_mode.lower() in ["a", "append"]:
            lib_writer.initialize_table(lib_struct.table, remove_existence=False)
        else:
            print(f"... [ERR] run_mode = {run_mode}")
            raise ValueError
        return lib_writer

    def get_lib_reader(self) -> CManagerLibReader:
        lib_struct = self.get_lib_struct()
        lib_reader = CManagerLibReader(self.lib_save_dir, lib_struct.lib_name)
        lib_reader.set_default(lib_struct.table.table_name)
        return lib_reader


# -----------------------------------------
# ---- some frequently used lib struct ----
# -----------------------------------------
class CLibFactor(CQuickSqliteLib):
    def __init__(self, factor: str, lib_save_dir: str):
        self.factor = factor
        super().__init__(lib_name=f"{self.factor}.db", lib_save_dir=lib_save_dir)

    def get_lib_struct(self) -> CLib1Tab1:
        return CLib1Tab1(
            lib_name=self.lib_name,
            table=CTable({
                "table_name": self.factor,
                "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
                "value_columns": {"value": "REAL"},
            })
        )


class CLibAvailableUniverse(CQuickSqliteLib):
    def __init__(self, lib_save_dir: str, lib_name: str = "available_universe.db"):
        super().__init__(lib_name, lib_save_dir)

    def get_lib_struct(self) -> CLib1Tab1:
        return CLib1Tab1(
            lib_name=self.lib_name,
            table=CTable({
                "table_name": "available_universe",
                "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
                "value_columns": {"return": "REAL", "amount": "REAL"},
            })
        )


class CLibMajorMinor(CQuickSqliteLib):
    def __init__(self, instrument: str, lib_save_dir: str, lib_name: str = "major_minor.db"):
        self.instrument = instrument
        super().__init__(lib_name, lib_save_dir)

    def get_lib_struct(self) -> CLib1Tab1:
        return CLib1Tab1(
            lib_name=self.lib_name,
            table=CTable({
                "table_name": self.instrument.replace(".", "_"),
                "primary_keys": {"trade_date": "TEXT"},
                "value_columns": {"n_contract": "TEXT", "d_contract": "TEXT"},
            })
        )


class CLibMajorReturn(CQuickSqliteLib):
    def __init__(self, instrument: str, lib_save_dir: str, lib_name: str = "major_return.db"):
        self.instrument = instrument
        super().__init__(lib_name, lib_save_dir)

    def get_lib_struct(self) -> CLib1Tab1:
        return CLib1Tab1(
            lib_name=self.lib_name,
            table=CTable({
                "table_name": self.instrument.replace(".", "_"),
                "primary_keys": {"trade_date": "TEXT"},
                "value_columns": {
                    "n_contract": "TEXT",
                    "prev_close": "REAL",
                    "open": "REAL",
                    "high": "REAL",
                    "low": "REAL",
                    "close": "REAL",
                    "volume": "INTEGER",
                    "amount": "REAL",
                    "oi": "INTEGER",
                    "major_return": "REAL",
                    "instru_idx": "REAL",
                    "openC": "REAL",
                    "highC": "REAL",
                    "lowC": "REAL",
                    "closeC": "REAL",
                },
            })
        )
