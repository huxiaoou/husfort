import numpy as np
import pandas as pd
from dataclasses import dataclass
from enum import IntEnum, StrEnum
from loguru import logger
from husfort.qcalendar import CCalendar
from husfort.qutility import check_and_makedirs
from husfort.qinstruments import CInstruMgr
from husfort.qsqlite import CMgrSqlDb, CDbStruct, CSqlTable, CSqlVar


def gen_nav_db(save_dir: str, save_id: str) -> CDbStruct:
    return CDbStruct(
        db_save_dir=save_dir,
        db_name=f"{save_id}.db",
        table=CSqlTable(
            name="nav",
            primary_keys=[CSqlVar("trade_date", "TEXT")],
            value_columns=[
                CSqlVar("init_cash", "REAL"),
                CSqlVar("tot_realized_pnl", "REAL"),
                CSqlVar("this_day_realized_pnl", "REAL"),
                CSqlVar("this_day_cost", "REAL"),
                CSqlVar("tot_unrealized_pnl", "REAL"),
                CSqlVar("last_nav", "REAL"),
                CSqlVar("nav", "REAL"),
                CSqlVar("navps", "REAL"),
                CSqlVar("ret", "REAL"),
            ]
        )
    )


class TExePriceType(StrEnum):
    OPEN = "open"
    CLOSE = "close"


class TPosDirection(IntEnum):
    LNG = 1
    SRT = -1


class TPosOffset(IntEnum):
    OPN = 1
    CLS = -1


@dataclass(frozen=True)
class CPosKey:
    contract: str
    direction: TPosDirection


"""
------ position ------
"""


@dataclass(frozen=True)
class CTrade:
    key: CPosKey
    offset: TPosOffset
    qty: int
    multiplier: int | float
    exe_price: float

    def cost(self, cost_rate: float) -> float:
        return self.exe_price * self.multiplier * self.qty * cost_rate


@dataclass
class CPosition:
    key: CPosKey
    qty: int
    multiplier: int | float
    cost_price: float
    last_price: float

    @property
    def unrealized_pnl(self) -> float:
        return (self.last_price - self.cost_price) * self.multiplier * self.qty * float(self.key.direction)

    def cal_trade_from_target(self, target: "CPosition", exe_price: float) -> CTrade:
        return CTrade(
            key=self.key,
            offset=TPosOffset.OPN if (self.qty <= target.qty) else TPosOffset.CLS,
            qty=abs(target.qty - self.qty),
            multiplier=self.multiplier,
            exe_price=exe_price,
        )

    def convert_as_trade(self, exe_price: float, operation: TPosOffset) -> CTrade:
        return CTrade(
            key=self.key,
            offset=operation,
            qty=self.qty,
            multiplier=self.multiplier,
            exe_price=exe_price,
        )

    def update_from_trade(self, trade: CTrade, cost_rate: float) -> tuple[float, float]:
        cost = trade.cost(cost_rate)
        if trade.offset == TPosOffset.OPN:
            sum_amt = (self.cost_price * self.qty + trade.exe_price * trade.qty)
            sum_qty = self.qty + trade.qty
            self.cost_price = sum_amt / sum_qty
            self.qty = sum_qty
            realized_pnl = 0
        elif trade.offset == TPosOffset.CLS:
            if trade.qty > self.qty:
                raise ValueError(f"trade qty {trade.qty} > pos qty {self.qty}")
            else:
                self.qty -= trade.qty
            realized_pnl = (trade.exe_price - self.cost_price) * self.multiplier * trade.qty * float(self.key.direction)
        else:
            raise ValueError(f"unknown offset = {trade.offset}")
        return realized_pnl, cost

    def update_from_market(self, last_price: float):
        self.last_price = last_price
        return 0


"""
------ manger major contract ------ 
"""


class CMgrMajContract:
    def __init__(self, universe: list[str], preprocess: CDbStruct):
        self.major_data: dict[str, dict[str, str]] = {}
        for instrument in universe:
            db_struct = preprocess.copy_to_another(another_db_name=f"{instrument}.db")
            sqldb = CMgrSqlDb(
                db_save_dir=db_struct.db_save_dir,
                db_name=db_struct.db_name,
                table=db_struct.table,
                mode="r",
            )
            data = sqldb.read(value_columns=["trade_date", "ticker_major"])
            self.major_data[instrument] = data.set_index("trade_date")["ticker_major"].to_dict()
        logger.info(f"Major contract loaded")

    def get_contract(self, trade_date: str, instrument: str) -> str:
        """

        :param trade_date: like "20250407"
        :param instrument: "CU.SHF"
        :return: "CU2506.SHF"
        """
        return self.major_data[instrument][trade_date]


"""
------ manger market data ------ 
"""


class CMgrMktData:
    def __init__(self, fmd: CDbStruct):
        sqldb = CMgrSqlDb(
            db_save_dir=fmd.db_save_dir,
            db_name=fmd.db_name,
            table=fmd.table,
            mode="r",
        )
        data = sqldb.read()
        data[["open", "close", "settle"]] = data[["open", "close", "settle"]].bfill(axis=1)
        keys = ["trade_date", "ts_code"]
        self.md: dict[tuple[str, str], dict] = data.set_index(keys).to_dict(orient="index")
        logger.info(f"Market data loaded")

    def get_md(self, trade_date: str, contract: str, md: str) -> int | float:
        """

        :param trade_date:
        :param contract:
        :param md:  ["pre_close", "pre_settle",
                     "open", "high", "low", "close", "settle",
                     "vol", "amount", "oi"]
        :return:
        """
        # return self.md[instrument][(trade_date, contract)][md]
        return self.md[(trade_date, contract)][md]


"""
------ signal reader ------
"""


class CSignal:
    def __init__(self, sid: str, signal_db_struct: CDbStruct):
        self.sid = sid
        sqldb = CMgrSqlDb(
            db_save_dir=signal_db_struct.db_save_dir,
            db_name=signal_db_struct.db_name,
            table=signal_db_struct.table,
            mode="r",
        )
        data = sqldb.read()
        self.signal: dict[str, dict[str, float]] = {}
        for trade_date, trade_date_data in data.groupby("trade_date"):
            trade_date: str
            trade_date_data: pd.DataFrame
            d: pd.DataFrame = trade_date_data[["instrument", "weight"]].set_index("instrument")
            self.signal[trade_date] = d["weight"].to_dict()

    def get_signal(self, trade_date: str) -> dict[str, float]:
        return self.signal[trade_date]


"""
------ account ------
"""

TPositions = dict[CPosKey, CPosition]


def print_positions(positions: TPositions):
    for key, pos in positions.items():
        print(f"{key}: {pos}")
    return 0


class CAccount:
    def __init__(self, init_cash: float, cost_rate: float):
        self.init_cash = init_cash
        self.cost_rate = cost_rate
        self.tot_realized_pnl: float = 0
        self.tot_unrealized_pnl: float = 0
        self.positions: TPositions = {}
        self.snapshots: list[dict] = []
        self.last_nav: float = init_cash

    @property
    def nav(self) -> float:
        return self.init_cash + self.tot_realized_pnl + self.tot_unrealized_pnl

    @property
    def navps(self) -> float:
        return self.nav / self.init_cash

    @property
    def ret(self) -> float:
        return self.nav / self.last_nav - 1

    def update_pnl(self, this_day_unrealized_pnl: float, this_day_realized_pnl: float, this_day_cost: float):
        self.tot_realized_pnl += (this_day_realized_pnl - this_day_cost)
        self.tot_unrealized_pnl = this_day_unrealized_pnl
        return 0

    def take_snapshot(self, trade_date: str, this_day_realized_pnl: float, this_day_cost: float):
        snapshot = {
            "trade_date": trade_date,
            "init_cash": self.init_cash,
            "tot_realized_pnl": self.tot_realized_pnl,
            "this_day_realized_pnl": this_day_realized_pnl,
            "this_day_cost": this_day_cost,
            "tot_unrealized_pnl": self.tot_unrealized_pnl,
            "last_nav": self.last_nav,
            "nav": self.nav,
            "navps": self.navps,
            "ret": self.ret,
        }
        self.snapshots.append(snapshot)
        return 0

    def update_last_nav(self):
        self.last_nav = self.nav
        return 0

    def export_snapshots(self) -> pd.DataFrame:
        return pd.DataFrame(self.snapshots)


"""
------ simulation ------
"""


class CSimulation:
    def __init__(
            self,
            signal: CSignal,
            init_cash: float,
            cost_rate: float,
            exe_price_type: TExePriceType,
            mgr_instru: CInstruMgr,
            mgr_maj_contract: CMgrMajContract,
            mgr_mkt_data: CMgrMktData,
            sim_save_dir: str
    ):
        self.signal: CSignal = signal
        self.account: CAccount = CAccount(init_cash, cost_rate)
        self.exe_price_type: TExePriceType = exe_price_type
        self.mgr_instru: CInstruMgr = mgr_instru
        self.mgr_maj_contract: CMgrMajContract = mgr_maj_contract
        self.mgr_mkt_data: CMgrMktData = mgr_mkt_data
        self.sim_save_dir = sim_save_dir

    def save_nav(self, nav_data: pd.DataFrame, calendar: CCalendar):
        check_and_makedirs(self.sim_save_dir)
        db_struct = gen_nav_db(self.sim_save_dir, save_id=self.signal.sid)
        sqldb = CMgrSqlDb(
            db_save_dir=db_struct.db_save_dir,
            db_name=db_struct.db_name,
            table=db_struct.table,
            mode="a",
        )
        if sqldb.check_continuity(incoming_date=nav_data["trade_date"].iloc[0], calendar=calendar) == 0:
            sqldb.update(update_data=nav_data)
        return 0

    @staticmethod
    def gen_sig_exe_dates(bgn_date: str, stp_date: str, calendar: CCalendar) -> tuple[list[str], list[str]]:
        sig_bgn_date = calendar.get_next_date(bgn_date, shift=-1)
        iter_dates = calendar.get_iter_list(sig_bgn_date, stp_date)
        sig_dates, exe_dates = iter_dates[0:-1], iter_dates[1:]
        return sig_dates, exe_dates

    def covert_sig_to_target_pos(self, sig_date: str) -> TPositions:
        sigs = self.signal.get_signal(sig_date)
        target_pos: TPositions = {}
        for instru, weight in sigs.items():
            multiplier = self.mgr_instru.get_multiplier(instru)
            contract = self.mgr_maj_contract.get_contract(sig_date, instru)
            sig_price = self.mgr_mkt_data.get_md(sig_date, contract, md="close")  # use 'close' to estimate qty
            qty = int(np.round(self.account.last_nav * abs(weight) / multiplier / sig_price))
            key = CPosKey(contract, direction=TPosDirection.LNG if weight > 0 else TPosDirection.SRT)
            target_pos[key] = CPosition(
                key=key, qty=qty, multiplier=multiplier,
                cost_price=0, last_price=0,
            )
        return target_pos

    def cal_trades(self, target_pos: TPositions, trade_date: str) -> list[CTrade]:
        trades: list[CTrade] = []
        for pos_key, tgt_pos in target_pos.items():
            exe_price = self.mgr_mkt_data.get_md(trade_date, pos_key.contract, md=self.exe_price_type)
            act_pos = self.account.positions.get(pos_key, None)
            if act_pos is None:
                trade = tgt_pos.convert_as_trade(exe_price=exe_price, operation=TPosOffset.OPN)
            else:
                trade = act_pos.cal_trade_from_target(target=tgt_pos, exe_price=exe_price)
            trades.append(trade) if trade.qty > 0 else None

        for pos_key, act_pos in self.account.positions.items():
            if pos_key not in target_pos:
                exe_price = self.mgr_mkt_data.get_md(trade_date, pos_key.contract, md=self.exe_price_type)
                trade = act_pos.convert_as_trade(exe_price, operation=TPosOffset.CLS)
                trades.append(trade) if trade.qty > 0 else None
        return trades

    def update_from_trades(self, trades: list[CTrade]) -> tuple[float, float]:
        """

        :param trades:
        :return: realized_pnl, cost
        """
        realized_pnl, cost = 0.0, 0.0
        for trade in trades:
            if trade.key not in self.account.positions:
                if trade.offset == TPosOffset.CLS:
                    raise ValueError(f"Try to close a position not in account: {trade.key}")
                else:  # trade.operation == TPosOperation.OPN
                    self.account.positions[trade.key] = CPosition(
                        key=trade.key, qty=0, multiplier=trade.multiplier,
                        cost_price=0, last_price=0,
                    )
            trade_rpnl, trade_cost = self.account.positions[trade.key].update_from_trade(
                trade, cost_rate=self.account.cost_rate)
            realized_pnl += trade_rpnl
            cost += trade_cost
        return realized_pnl, cost

    def update_from_market(self, trade_date: str) -> float:
        """

        :param trade_date:
        :return: unrealized_pnl
        """

        unrealized_pnl = 0
        rm_keys: list[CPosKey] = []
        for pos_key, pos in self.account.positions.items():
            if pos.qty > 0:
                contract = pos_key.contract
                last_price = self.mgr_mkt_data.get_md(trade_date, contract=contract, md="close")
                pos.update_from_market(last_price=last_price)
                unrealized_pnl += pos.unrealized_pnl
            else:
                rm_keys.append(pos_key)
        for pos_key in rm_keys:
            del self.account.positions[pos_key]
        return unrealized_pnl

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar, verbose: bool = False):
        sig_dates, exe_dates = self.gen_sig_exe_dates(bgn_date, stp_date, calendar)
        for sig_date, exe_date in zip(sig_dates, exe_dates):
            target_pos = self.covert_sig_to_target_pos(sig_date=sig_date)
            trades = self.cal_trades(target_pos, trade_date=exe_date)
            this_day_realized_pnl, this_day_cost = self.update_from_trades(trades=trades)
            this_day_unrealized_pnl = self.update_from_market(trade_date=exe_date)
            self.account.update_pnl(
                this_day_unrealized_pnl=this_day_unrealized_pnl,
                this_day_realized_pnl=this_day_realized_pnl,
                this_day_cost=this_day_cost,
            )
            self.account.take_snapshot(exe_date, this_day_realized_pnl, this_day_cost)
            self.account.update_last_nav()
            if verbose:
                print(f"----------{exe_date}----------")
                print_positions(self.account.positions)
        snapshots = self.account.export_snapshots()
        self.save_nav(snapshots, calendar)
        return 0
