import pandas as pd
from husfort.qcalendar import CCalendar
from husfort.qsqlite import CDbStruct, CMgrSqlDb, CSqlVar, CSqlTable
from husfort.qplot import CPlotLines


def gen_sims_quick_nav_db(save_dir: str, save_id: str) -> CDbStruct:
    return CDbStruct(
        db_save_dir=save_dir,
        db_name=f"{save_id}.db",
        table=CSqlTable(
            name="nav",
            primary_keys=[CSqlVar("trade_date", "TEXT")],
            value_columns=[
                CSqlVar("raw_ret", "REAL"),
                CSqlVar("delta_weights_sum", "REAL"),
                CSqlVar("cost", "REAL"),
                CSqlVar("ret", "REAL"),
                CSqlVar("nav", "REAL"),
            ],
        )
    )


class CSignalsLoaderBase:
    def load(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        """

        :param bgn_date:
        :param stp_date:
        :return: a pd.DataFrame with columns ["trade_date", "instrument", "weight"]
        """
        raise NotImplementedError

    @property
    def signal_id(self) -> str:
        raise NotImplementedError


class CTestReturnLoaderBase:
    def load(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        """

        :param bgn_date:
        :param stp_date:
        :return: a pd.DataFrame with columns ["trade_date", "instrument", self.ret_name]
        """
        raise NotImplementedError

    @property
    def shift(self) -> int:
        raise NotImplementedError

    @property
    def ret_name(self) -> str:
        raise NotImplementedError


class CSimQuick:
    """
    This class provides a quick method to test signals using test_returns and signals only.
    0.  this class support increment test. In other words, user can use this class to do daily increment
        to get a quick result.
    1.  the results may be a slightly BETTER than the results in husfort.qsimulation because of:
        1.1 major contract shifting is NOT considered, less cost is calculated.
        1.2 a precise weight number instead of a specific quantity is used.
    2.  Always plot a nav curve since nav_plot_bgn_date.
    """

    def __init__(
            self,
            signals_loader: CSignalsLoaderBase,
            test_return_loader: CTestReturnLoaderBase,
            cost_rate: float,
            sims_quick_dir: str,
    ):
        """

        :param signals_loader: user should inherit it from CSignalsLoaderBase,
                               and provide details to methods: load and signal_id.
        :param test_return_loader: user should inherit it from CTestReturnLoaderBase,
                                   and provide details to methods: load, shift and ret_name.
        :param cost_rate:
        :param sims_quick_dir:
        """
        self.signals_loader = signals_loader
        self.test_return_loader = test_return_loader
        self.cost_rate = cost_rate
        self.quick_sim_save_dir = sims_quick_dir

    def get_dates(self, bgn_date: str, stp_date: str, calendar: CCalendar) -> tuple[list[str], list[str]]:
        d = self.test_return_loader.shift + 1  # +1 for calculating delta weights
        buffer_bgn_date = calendar.get_next_date(bgn_date, -d)
        iter_dates = calendar.get_iter_list(buffer_bgn_date, stp_date)
        sig_dates = iter_dates[0:-d + 1]
        exe_dates = iter_dates[(d - 1):]
        return sig_dates, exe_dates

    def get_sigs_and_rets(self, base_bgn_date: str, base_stp_date: str) -> pd.DataFrame:
        sigs = self.signals_loader.load(base_bgn_date, base_stp_date)
        rets = self.test_return_loader.load(base_bgn_date, base_stp_date)
        data = pd.merge(left=sigs, right=rets, how="right", on=["trade_date", "instrument"]).fillna(0)
        return data

    def cal_core(self, data: pd.DataFrame, exe_dates: list[str]) -> pd.DataFrame:
        raw_ret = data.groupby(by="trade_date").apply(lambda z: z["weight"] @ z[self.test_return_loader.ret_name])
        daily_weights = pd.pivot_table(data=data, index="trade_date", columns="instrument", values="weight")
        delta_weights = daily_weights.diff().fillna(0)
        delta_weights_sum = delta_weights.abs().sum(axis=1)
        cost = delta_weights_sum * self.cost_rate
        net_ret = raw_ret - cost
        result = pd.DataFrame({
            "raw_ret": raw_ret,
            "delta_weights_sum": delta_weights_sum,
            "cost": cost,
            "ret": net_ret,
            "exe_date": exe_dates,
        })
        return result

    @staticmethod
    def recalibrate_dates(raw_result: pd.DataFrame, bgn_date: str) -> pd.DataFrame:
        net_result = raw_result.set_index("exe_date").truncate(before=bgn_date)
        net_result.index.name = "trade_date"
        return net_result

    @staticmethod
    def update_nav(net_result: pd.DataFrame, last_nav: float):
        net_result["nav"] = (net_result["ret"] + 1).cumprod() * last_nav
        return 0

    def load_nav_at_date(self, trade_date: str) -> float:
        db_struct = gen_sims_quick_nav_db(save_dir=self.quick_sim_save_dir, save_id=self.signals_loader.signal_id)
        sqldb = CMgrSqlDb(
            db_save_dir=self.quick_sim_save_dir,
            db_name=db_struct.db_name,
            table=db_struct.table,
            mode="r",
        )
        last_nav = 1.00
        if sqldb.has_table(db_struct.table):
            nav_data = sqldb.read_by_conditions(
                conditions=[("trade_date", "=", trade_date)],
                value_columns=["trade_date", "nav"],
            )
            if not nav_data.empty:
                last_nav = nav_data["nav"].values[0]
        return last_nav

    def load_nav_range(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        db_struct = gen_sims_quick_nav_db(save_dir=self.quick_sim_save_dir, save_id=self.signals_loader.signal_id)
        sqldb = CMgrSqlDb(
            db_save_dir=self.quick_sim_save_dir,
            db_name=db_struct.db_name,
            table=db_struct.table,
            mode="r",
        )
        nav_data = sqldb.read_by_range(
            bgn_date=bgn_date, stp_date=stp_date,
            value_columns=["trade_date", "nav"],
        )
        return nav_data.set_index("trade_date")

    def save_nav(self, net_result: pd.DataFrame, calendar: CCalendar):
        db_struct = gen_sims_quick_nav_db(save_dir=self.quick_sim_save_dir, save_id=self.signals_loader.signal_id)
        sqldb = CMgrSqlDb(
            db_save_dir=self.quick_sim_save_dir,
            db_name=db_struct.db_name,
            table=db_struct.table,
            mode="a",
        )
        if sqldb.check_continuity(net_result.index[0], calendar) == 0:
            sqldb.update(update_data=net_result, using_index=True)
        return 0

    def plot(self, nav_data: pd.DataFrame):
        artist = CPlotLines(
            plot_data=nav_data,
            fig_name=f"{self.signals_loader.signal_id}",
            fig_save_dir=self.quick_sim_save_dir,
        )
        artist.plot()
        artist.set_axis_x(xtick_count=12)
        artist.save_and_close()
        return 0

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar, nav_plot_bgn_date: str = "20120104"):
        sig_dates, exe_dates = self.get_dates(bgn_date, stp_date, calendar)
        base_bgn_date, base_stp_date = sig_dates[0], calendar.get_next_date(sig_dates[-1], shift=1)
        last_date = calendar.get_next_date(bgn_date, shift=-1)
        data = self.get_sigs_and_rets(base_bgn_date, base_stp_date)
        raw_result = self.cal_core(data, exe_dates)
        net_result = self.recalibrate_dates(raw_result, bgn_date)
        last_nav = self.load_nav_at_date(trade_date=last_date)
        self.update_nav(net_result, last_nav)
        self.save_nav(net_result, calendar)
        nav_data = self.load_nav_range(bgn_date=nav_plot_bgn_date, stp_date=stp_date)
        self.plot(nav_data=nav_data)
        return 0


class CSignalsLoader(CSignalsLoaderBase):
    def __init__(self, sid: str, signal_db_struct: CDbStruct):
        self._sid = sid
        sqldb = CMgrSqlDb(
            db_save_dir=signal_db_struct.db_save_dir,
            db_name=signal_db_struct.db_name,
            table=signal_db_struct.table,
            mode="r",
        )
        self.data = sqldb.read()

    @property
    def signal_id(self):
        return self._sid

    def load(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        return self.data.query(f"trade_date >= '{bgn_date}' and trade_date < '{stp_date}'")
