import pandas as pd
import sqlite3 as sql3
import argparse
from husfort.qutility import SFG
from husfort.qlog import logger, define_logger

define_logger()


class __CDataViewer:
    def __init__(self):
        self.raw_data: pd.DataFrame = pd.DataFrame()
        self.slc_data: pd.DataFrame = pd.DataFrame()

    def fetch(self, cols: list[str], where: str):
        raise NotImplementedError

    def pick_head_tail(self, head: int, tail: int):
        if head > 0:
            if tail > 0:
                print(
                    f"[INF] both argument head and tail are given, (head, tail)=({head}, {tail}). "
                    f"A concat data will be generated. If total length of data {len(self.slc_data)} < {head + tail}, "
                    f"the result may be overlapped."
                )
                self.slc_data = pd.concat(
                    objs=[self.slc_data.head(head), self.slc_data.tail(tail)],
                    axis=0,
                    ignore_index=False
                )
            else:
                self.slc_data = self.slc_data.head(head)
        else:
            if tail > 0:
                self.slc_data = self.slc_data.tail(tail)
            else:
                # self.slc_data = self.slc_data
                pass
        return 0

    def pick_chead_ctail(self, chead: int, ctail: int):
        if chead > 0:
            if ctail > 0:
                print(
                    f"[INF] both argument chead and ctail are given, (chead, ctail)=({chead}, {ctail}). "
                    f"A concat data will be generated. If total width of data {self.slc_data.shape[1]} < {chead + ctail}, "
                    f"the result may be overlapped."
                )
                self.slc_data = pd.concat(
                    objs=[self.slc_data.iloc[:, 0:chead], self.slc_data.iloc[:, -ctail:]],
                    axis=1,
                    ignore_index=False
                )
            else:
                self.slc_data = self.slc_data.iloc[:, 0:chead]
        else:
            if ctail > 0:
                self.slc_data = self.slc_data.iloc[:, -ctail:]
            else:
                pass
        return 0

    def sort(self, sort: list[str], ascending: list[bool]):
        if sort:
            self.slc_data = self.slc_data.sort_values(sort, ascending=ascending)
        return 0

    def show(
            self,
            head: int, tail: int,
            chead: int, ctail: int,
            sort: list[str], ascending: list[bool],
            max_rows: int, max_cols: int,
            transpose: bool = False,
    ):
        self.sort(sort=sort, ascending=ascending)
        self.pick_head_tail(head=head, tail=tail)
        self.pick_chead_ctail(chead=chead, ctail=ctail)
        pd.set_option("display.unicode.east_asian_width", True)
        if max_rows > 0:
            pd.set_option("display.max_rows", max_rows)
        if max_cols > 0:
            pd.set_option("display.max_columns", max_cols)
        if transpose:
            print(self.slc_data.T)
        else:
            print(self.slc_data)
        return

    def save(self, save_path: str, index: bool, float_format: str):
        if save_path:
            if save_path.endswith(".csv"):
                self.slc_data.to_csv(save_path, index=index, float_format=float_format)
            elif save_path.endswith(".xls") or save_path.endswith(".xlsx"):
                self.slc_data.to_excel(save_path, index=index, float_format=float_format)
        return


class CDataViewerCSV(__CDataViewer):
    def __init__(self, src_path: str, sheet_name: int | str, header: int):
        super().__init__()
        self.src_path = src_path
        self.sheet_name = sheet_name
        self.header = header

    def fetch(self, cols: list[str], where: str):
        if self.src_path.endswith(".xls") or self.src_path.endswith(".xlsx"):
            self.raw_data = pd.read_excel(
                self.src_path,
                sheet_name=self.sheet_name,
                header=self.header if self.header >= 0 else None,
            )
        else:
            self.raw_data = pd.read_csv(
                self.src_path,
                header=self.header if self.header >= 0 else None,
            )
        if where:
            self.slc_data = self.raw_data.query(where)
        else:
            self.slc_data = self.raw_data
        if cols:
            self.slc_data = self.slc_data[cols]
        return


class CDataViewerSql(__CDataViewer):
    def __init__(self, lib: str, table: str = None):
        super().__init__()
        self.lib = lib
        self.table = table or self.get_tables()[0]

    def get_tables(self) -> list[str]:
        with sql3.connect(self.lib) as connection:
            cursor = connection.cursor()
            sql = "SELECT name FROM sqlite_master WHERE type='table';"
            cursor.execute(sql)
            tables = cursor.fetchall()
            return [z[0] for z in tables]

    def get_var_names_from_table(self) -> list[str]:
        with sql3.connect(self.lib) as connection:
            cursor = connection.cursor()
            sql = f"select * from {self.table} where 1=0;"
            cursor.execute(sql)
            _names = [d[0] for d in cursor.description]
        return _names

    def fetch(self, cols: list[str], where: str):
        var_str = ",".join(col_names := (cols or self.get_var_names_from_table()))
        with sql3.connect(self.lib) as connection:
            cursor = connection.cursor()
            try:
                cmd_sql = f"SELECT {var_str} from {self.table} {f'WHERE {where}' if where else ''}"
                data = cursor.execute(cmd_sql).fetchall()
                self.slc_data = pd.DataFrame(data, columns=col_names)
            except sql3.OperationalError:
                logger.info(
                    f"argument --where='{SFG(where)}' may not supported by sqlite3 directly, "
                    f"program will try pd.DataFrame.query method"
                )
                cmd_sql = f"SELECT {var_str} from {self.table}"
                data = cursor.execute(cmd_sql).fetchall()
                self.slc_data = pd.DataFrame(data, columns=col_names).query(where)
        return


class CDataViewerH5(__CDataViewer):
    def __init__(self, lib: str, table: str):
        super().__init__()
        self.lib = lib
        self.table = table

    def fetch(self, cols: list[str], where: str):
        with pd.HDFStore(path=self.lib, mode="r") as store:
            if where:
                self.slc_data = store.select(key=self.table, where=where.split(";"))  # type:ignore
            else:
                self.slc_data = store.get(key=self.table)
        if cols:
            self.slc_data = self.slc_data[cols]
        return


"""
------------------------------------
------- class for ArgsParser -------
------------------------------------
"""


class CArgsParserViewer:
    def __init__(self, desc: str):
        self.args_parser = argparse.ArgumentParser(description=desc)

    def add_arguments(self):
        self.args_parser.add_argument(
            "--vars",
            type=str,
            default=None,
            help="variables to fetch, separated by ',' like \"open,high,low,close\", "
                 "if not provided then fetch all.",
        )
        self.args_parser.add_argument(
            "--where",
            type=str,
            default=None,
            help="conditions to filter, sql expression "
                 "like \"(instrument = 'a' OR instrument = 'd') AND (trade_date <= '20120131')\". "
                 "For h5 viewer,  multiple conditions are separated by ';'."
                 "like \"instrument = 'a' | instrument = 'd';trade_date <= '20120131'\""
        )
        self.args_parser.add_argument(
            "--sort",
            type=str,
            default=None,
            help="columns to sort, separated by ',', like 'trade_date,instrument,close'"
        )
        self.args_parser.add_argument(
            "--ascending",
            type=str,
            default=None,
            help="works only --sort is provided, 'T' for ascending, 'F' for descending, like 'T,F,T'. "
                 "All would be ='T' if not provided."
        )

        self.args_parser.add_argument(
            "--head", type=int, default=0, help="integer, head rows to print"
        )
        self.args_parser.add_argument(
            "--tail", type=int, default=0, help="integer, tail rows to print"
        )
        self.args_parser.add_argument(
            "--chead", type=int, default=0, help="integer, head columns to print"
        )
        self.args_parser.add_argument(
            "--ctail", type=int, default=0, help="integer, tail columns to print"
        )
        self.args_parser.add_argument(
            "--maxrows",
            type=int,
            default=0,
            help="integer, provide larger value to see more rows when print outcomes",
        )
        self.args_parser.add_argument(
            "--maxcols",
            type=int,
            default=0,
            help="integer, provide larger value to see more columns when print outcomes",
        )
        self.args_parser.add_argument(
            "--transpose",
            default=False,
            action="store_true",
            help="boolean, transpose the outcomes when show if activated",
        )
        self.args_parser.add_argument(
            "--save",
            type=str,
            default=None,
            help="a path to save the resulting data if provided",
        )
        self.args_parser.add_argument(
            "--index",
            default=False,
            action="store_true",
            help="boolean, save index when saving if activated",
        )
        self.args_parser.add_argument(
            "--floatfmt",
            default="%.6f",
            help="float format when saving, default is '%%.6f'",
        )

    def get_args(self):
        return self.args_parser.parse_args()

    @staticmethod
    def parse_vars(variables: str):
        return variables.split(",") if variables else []

    @staticmethod
    def parse_sorts(sort: str, ascending: str) -> tuple[list[str], list[bool]]:
        sorts = sort.split(",") if sort else []
        if ascending:
            ascendings = [z.upper() in ["T", "TRUE"] for z in ascending.split(",")]
        else:
            ascendings = [True] * len(sorts)
        if len(ascendings) >= len(sorts):
            return sorts, ascendings[0:len(sorts)]
        else:  # len(ascendings) < len(sorts)
            if ascendings:
                return sorts, ascendings + [ascendings[-1]] * (len(sorts) - len(ascendings))
            else:
                return sorts, [True] * len(sorts)


def int_or_str(arg):
    try:
        return int(arg)
    except ValueError:
        return arg


class CArgsParserViewerCsv(CArgsParserViewer):
    def __init__(self):
        super().__init__(desc="A program to view data in csv/xls/xlsx files")

    def add_arguments(self):
        super().add_arguments()
        self.args_parser.add_argument(
            "path",
            type=str,
            help="path for csv file, like 'E:\\tmp\\test.csv.gz' or 'test.csv'",
        )
        self.args_parser.add_argument(
            "--sheet",
            type=int_or_str,
            default=0,
            help="string for name of sheet, or integers are used in zero-indexed sheet positions. Only required for reading xls/xlsx files",
        )
        self.args_parser.add_argument(
            "--header",
            type=int,
            default=0,
            help="row number of headers, use -1 if there is no header in the source file"
        )


class CArgsParserViewerSql(CArgsParserViewer):
    def __init__(self):
        super().__init__(desc="A program to view data in sqlite.db")

    def add_arguments(self):
        super().add_arguments()
        self.args_parser.add_argument(
            "lib",
            type=str,
            help="path for sql file, like 'E:\\tmp\\alternative.db'",
        )
        self.args_parser.add_argument(
            "--table",
            type=str,
            default=None,
            help="table name in the sql file, like 'macro' or 'forex' in alternative.db. If not provided, the first table will be used.",
        )


class CArgsParserViewerH5(CArgsParserViewer):
    def __init__(self):
        super().__init__(desc="A program to view data in h5")

    def add_arguments(self):
        super().add_arguments()
        self.args_parser.add_argument(
            "lib",
            type=str,
            help="path for h5 file, like 'E:\\tmp\\test.h5'",
        )
        self.args_parser.add_argument(
            "--table",
            type=str,
            required=True,
            help="table name in the h5 file, like '/grp1/grp2/testTable'",
        )
