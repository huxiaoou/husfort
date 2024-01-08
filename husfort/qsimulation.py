import os
import numpy as np
import pandas as pd
from husfort.qsqlite import CManagerLibReader, CLibFactor
from husfort.qcalendar import CCalendar
from husfort.qinstruments import (CPosKey, CContract, TOperation, CONST_DIRECTION_LNG, CONST_DIRECTION_SRT, CONST_OPERATION_OPN, CONST_OPERATION_CLS)


# ------------------------------------------ Classes general -------------------------------------------------------------------
class CManagerMajor(object):
    def __init__(self, mother_universe: list[str], major_minor_dir: str, major_minor_lib_name: str = "major_minor.db"):
        src_db_reader = CManagerLibReader(major_minor_dir, major_minor_lib_name)
        self.m_major: dict[str, pd.DataFrame] = {}
        for instrument in mother_universe:
            instrument_major_data_df = src_db_reader.read(
                value_columns=["trade_date", "n_contract"],
                using_default_table=False,
                table_name=instrument.replace(".", "_"))
            self.m_major[instrument] = instrument_major_data_df.set_index("trade_date")
        src_db_reader.close()

    def inquiry_major_contract(self, instrument: str, trade_date: str) -> str:
        return self.m_major[instrument].at[trade_date, "n_contract"]


class CManagerMarketData(object):
    def __init__(self, mother_universe: list[str], market_data_dir: str, market_data_file_name_tmpl: str = "{}.md.{}.csv.gz",
                 price_types: tuple[str] = ("open", "close", "settle")):
        self.md: dict[str, dict[str, pd.DataFrame]] = {p: {} for p in price_types}
        for prc_type in self.md:
            for instrument_id in mother_universe:
                instrument_md_file = market_data_file_name_tmpl.format(instrument_id, prc_type)
                instrument_md_path = os.path.join(market_data_dir, instrument_md_file)
                instrument_md_df = pd.read_csv(instrument_md_path, dtype={"trade_date": str}).set_index("trade_date")
                self.md[prc_type][instrument_id] = instrument_md_df

    def inquiry_price_at_date(self, contact: str, instrument: str, trade_date: str, price_type: str = "close") -> float:
        return self.md[price_type][instrument].at[trade_date, contact]


class CManagerSignal(object):
    def __init__(self, factor: str, universe: list[str], factors_dir: str,
                 mgr_md: CManagerMarketData, mgr_major: CManagerMajor):
        """

        :param factor:
        :param universe: list of all instruments, instrument not in this list can not be traded
        :param factors_dir:
        :param mgr_md:
        :param mgr_major:

        """

        self.factor = factor
        self.universe: set = set(universe)
        self.factor_lib: CManagerLibReader = CLibFactor(factor=factor, lib_save_dir=factors_dir).get_lib_reader()
        self.mgr_md: CManagerMarketData = mgr_md
        self.mgr_major: CManagerMajor = mgr_major

    def close_libs(self):
        self.factor_lib.close()
        return 0

    def cal_position_header(self, sig_date: str) -> pd.DataFrame:
        # --- load factors at signal date
        factor_df = self.factor_lib.read_by_date(sig_date, value_columns=["instrument", "value"])
        factor_df = factor_df.rename(mapper={"value": self.factor}, axis=1).set_index("instrument")

        # --- selected/optimized universe
        header_universe = [_ for _ in self.universe if _ in factor_df.index]
        header_weight_df: pd.DataFrame = factor_df.loc[header_universe]

        if header_weight_df.empty:
            return pd.DataFrame(data=None, columns=["contract", "price", "direction", "weight"])
        else:
            header_weight_df.reset_index(inplace=True)  # columns = ["instrument", "factor", self.factor]
            header_weight_df.sort_values(by=[self.factor, "instrument"], ascending=[False, True], inplace=True)
            header_weight_df["contract"] = header_weight_df["instrument"].map(lambda z: self.mgr_major.inquiry_major_contract(z, sig_date))
            header_weight_df["price"] = header_weight_df[["instrument", "contract"]].apply(
                lambda z: self.mgr_md.inquiry_price_at_date(z["contract"], z["instrument"], sig_date), axis=1)
            header_weight_df["direction"] = header_weight_df[self.factor].map(lambda z: int(np.sign(z)))
            header_weight_df["weight"] = header_weight_df[self.factor].abs()
            return header_weight_df[["contract", "price", "direction", "weight"]]


# ------------------------------------------ Classes about trades -------------------------------------------------------------------
class CTrade(object):
    def __init__(self, pos_key: CPosKey, operation: TOperation, quantity: int):
        """

        :param pos_key
        :param operation:
        :param quantity:
        """
        self._pos_key = pos_key
        self._operation: TOperation = operation
        self._quantity: int = quantity
        self._executed_price: float = 0

    @property
    def pos_key(self) -> CPosKey:
        return self._pos_key

    @property
    def contract_and_instru_id(self) -> tuple[str, str]:
        return self._pos_key.contract_and_instru_id

    @property
    def execution(self) -> tuple[TOperation, int, float]:
        return self._operation, self._quantity, self._executed_price

    @property
    def operation_is_opn(self) -> bool:
        return self._operation == CONST_OPERATION_OPN

    @property
    def operation_is_cls(self) -> bool:
        return self._operation == CONST_OPERATION_CLS

    @property
    def executed_price(self) -> float:
        return self._executed_price

    @executed_price.setter
    def executed_price(self, executed_price: float):
        self._executed_price = executed_price


# --- Class: Position
class CPosition(object):
    def __init__(self, pos_key: CPosKey):
        self._pos_key: CPosKey = pos_key
        self._quantity: int = 0

    def cal_quantity(self, price: float, allocated_mkt_val: float):
        self._quantity = int(np.round(allocated_mkt_val / price / self._pos_key.contract.contract_multiplier))
        return 0

    @property
    def pos_key(self) -> CPosKey:
        return self._pos_key

    @property
    def pos_id(self) -> tuple[str, str]:
        return self.pos_key.contract.contract, self.pos_key.contract.instrument

    @property
    def quantity(self) -> int:
        return self._quantity

    @property
    def is_empty(self) -> bool:
        return self._quantity == 0

    def cal_trade_from_other_pos(self, other: "CPosition") -> CTrade | None:
        """

        :param other: another position unit, usually new(target) position, must have the same key as self
        :return: None or a new trade
        """
        new_trade: CTrade | None = None
        delta_quantity: int = other.quantity - self.quantity
        if delta_quantity > 0:
            new_trade: CTrade = CTrade(pos_key=self.pos_key, operation=CONST_OPERATION_OPN, quantity=delta_quantity)
        elif delta_quantity < 0:
            new_trade: CTrade = CTrade(pos_key=self.pos_key, operation=CONST_OPERATION_CLS, quantity=-delta_quantity)
        return new_trade

    def open(self):
        new_trade: CTrade = CTrade(pos_key=self.pos_key, operation=CONST_OPERATION_OPN, quantity=self.quantity)
        return new_trade

    def close(self):
        new_trade: CTrade = CTrade(pos_key=self.pos_key, operation=CONST_OPERATION_CLS, quantity=self.quantity)
        return new_trade

    def to_dict(self) -> dict:
        return {
            "contact": self.pos_key.contract.contract,
            "direction": self.pos_key.direction,
            "quantity": self.quantity,
            "contract_multiplier": self.pos_key.contract.contract_multiplier,
        }


# --- Class: PositionPlus
class CPositionPlus(CPosition):
    def __init__(self, pos_key: CPosKey, cost_rate: float):
        super().__init__(pos_key=pos_key)
        self.cost_price: float = 0
        self.last_price: float = 0
        self.unrealized_pnl: float = 0
        self.cost_rate: float = cost_rate

    def update_from_trade(self, trade: CTrade) -> dict:
        operation, quantity, executed_price = trade.execution
        cost = executed_price * quantity * self.pos_key.contract.contract_multiplier * self.cost_rate
        realized_pnl = 0
        if operation == CONST_OPERATION_OPN:
            amt_new = self.cost_price * self._quantity + executed_price * quantity
            self._quantity += quantity
            self.cost_price = amt_new / self._quantity
        elif operation == CONST_OPERATION_CLS:
            realized_pnl = (executed_price - self.cost_price) * self.pos_key.direction * self.pos_key.contract.contract_multiplier * quantity
            self._quantity -= quantity
        else:
            print(f"operation = {operation} is illegal")
            raise ValueError

        return {
            "contract": self.pos_key.contract.contract,
            "direction": self.pos_key.direction,
            "operation": operation,
            "quantity": quantity,
            "price": executed_price,
            "cost": cost,
            "realized_pnl": realized_pnl,
        }

    def update_from_market_data(self, price: float) -> float:
        self.last_price = price
        self.unrealized_pnl = (self.last_price - self.cost_price) * self.pos_key.direction * self.pos_key.contract.contract_multiplier * self._quantity
        return self.unrealized_pnl

    def update_from_last(self) -> float:
        self.unrealized_pnl = (self.last_price - self.cost_price) * self.pos_key.direction * self.pos_key.contract.contract_multiplier * self._quantity
        return self.unrealized_pnl

    def to_dict(self) -> dict:
        d = super().to_dict()
        d.update(
            {
                "cost_price": self.cost_price,
                "last_price": self.last_price,
                "unrealized_pnl": self.unrealized_pnl,
            }
        )
        return d


# --- Class: Portfolio
class CPortfolio(object):
    def __init__(self, pid: str, init_cash: float, cost_reservation: float, cost_rate: float,
                 dir_pid: str, dir_pid_trades: str, dir_pid_positions: str, verbose: bool = True):
        # basic
        self.pid: str = pid

        # pnl
        self.init_cash: float = init_cash
        self.realized_pnl_daily_details: list[dict] = []
        self.realized_pnl_daily_df: pd.DataFrame = pd.DataFrame()
        self.realized_pnl_daily: float = 0
        self.realized_pnl_cum: float = 0
        self.unrealized_pnl: float = 0
        self.nav: float = self.init_cash + self.realized_pnl_cum + self.unrealized_pnl

        # position
        self.manager_pos: dict[CPosKey, CPositionPlus] = {}

        # additional
        self.cost_reservation: float = cost_reservation
        self.cost_rate: float = cost_rate
        self.update_date: str = ""  # format =  "YYYYMMDD"

        # save nav
        self.nav_daily_snapshots = []
        self.verbose: bool = verbose
        self.dir_pid: str = dir_pid
        self.dir_pid_trades: str = dir_pid_trades
        self.dir_pid_positions: str = dir_pid_positions

    def cal_target_position(self, new_pos_df: pd.DataFrame) -> dict[CPosKey, CPosition]:
        """

        :param new_pos_df : a DataFrame with columns = ["contract", "price", "direction", "weight"]
                            a.1 this "price" is used to estimate how much quantity should be allocated
                                for the instrument, CLOSE-PRICE is most frequently used, but other types
                                such as OPEN could do the job as well. if new position is to open with T
                                day's price, this "price" should be from T-1, which is available.
                            a.2 direction: 1 for long, -1 for short.
                            a.3 weight: non-negative value, sum of weights should not be greater than 1, if
                                leverage are not allowed
        :return:
        """
        mgr_new_pos: dict[CPosKey, CPosition] = {}
        tot_allocated_amt = self.nav / (1 + self.cost_reservation)
        for contract, direction, price, weight in zip(new_pos_df["contract"], new_pos_df["direction"], new_pos_df["price"], new_pos_df["weight"]):
            if direction == 1:
                pos_key = CPosKey(contract, CONST_DIRECTION_LNG)
            elif direction == -1:
                pos_key = CPosKey(contract, CONST_DIRECTION_SRT)
            elif direction == 0:
                continue
            else:
                print(f"direction = {direction}")
                raise ValueError
            tgt_pos = CPosition(pos_key)
            tgt_pos.cal_quantity(price=price, allocated_mkt_val=tot_allocated_amt * weight)
            key, qty = tgt_pos.pos_key, tgt_pos.quantity
            if qty > 0:
                mgr_new_pos[key] = tgt_pos
        return mgr_new_pos

    def cal_trades_for_signal(self, mgr_new_pos: dict[CPosKey, CPosition]) -> list[CTrade]:
        trades: list[CTrade] = []
        # cross comparison: step 0, check if new position is in old position
        for new_key, new_pos in mgr_new_pos.items():
            if new_key not in self.manager_pos:
                new_trade: CTrade = new_pos.open()
            else:
                new_trade: CTrade = self.manager_pos[new_key].cal_trade_from_other_pos(other=new_pos)  # could be none
            if new_trade is not None:
                trades.append(new_trade)

        # cross comparison: step 1, check if old position is in new position
        for old_key, old_pos in self.manager_pos.items():
            if old_key not in mgr_new_pos:
                new_trade: CTrade = old_pos.close()
                trades.append(new_trade)
        return trades

    def cal_trades_for_major(self, mgr_major: CManagerMajor) -> list[CTrade]:
        trades: list[CTrade] = []
        for old_key, old_pos in self.manager_pos.items():
            old_contract_id, instrument = old_pos.pos_id
            new_contract_id = mgr_major.inquiry_major_contract(instrument=instrument, trade_date=self.update_date)
            if old_contract_id != new_contract_id:
                trade_close_old = old_pos.close()
                new_contract = CContract.gen_from_other(new_contract_id, old_key.contract)
                pos_key = CPosKey(new_contract, old_pos.pos_key.direction)
                trade_open_new = CTrade(pos_key=pos_key, operation=CONST_OPERATION_OPN, quantity=old_pos.quantity)
                trades.append(trade_close_old)
                trades.append(trade_open_new)
        return trades

    def update_from_trades(self, trades: list[CTrade]):
        # trades loop
        for trade in trades:
            trade_key = trade.pos_key
            if trade_key not in self.manager_pos:
                self.manager_pos[trade_key] = CPositionPlus(pos_key=trade_key, cost_rate=self.cost_rate)
            trade_result = self.manager_pos[trade_key].update_from_trade(trade=trade)
            self.realized_pnl_daily_details.append(trade_result)

        # remove empty positions
        for pos_key in self.manager_pos:
            if self.manager_pos[pos_key].is_empty:
                del self.manager_pos[pos_key]
        return 0

    def initialize_daily(self, trade_date: str) -> int:
        self.update_date = trade_date
        self.realized_pnl_daily_details = []
        self.realized_pnl_daily_df = None
        self.realized_pnl_daily = 0
        return 0

    def update_unrealized_pnl(self, mgr_md: CManagerMarketData) -> int:
        self.unrealized_pnl = 0
        for pos in self.manager_pos.values():
            contract, instrument = pos.pos_id
            last_price = mgr_md.inquiry_price_at_date(
                contact=contract, instrument=instrument, trade_date=self.update_date,
                price_type="close"
            )  # always use close to estimate the unrealized pnl
            if np.isnan(last_price):
                print(f"nan price for {contract} {self.update_date}")
                self.unrealized_pnl += pos.update_from_last()
            else:
                self.unrealized_pnl += pos.update_from_market_data(price=last_price)
        return 0

    def update_realized_pnl(self) -> int:
        if self.realized_pnl_daily_details:
            self.realized_pnl_daily_df = pd.DataFrame(self.realized_pnl_daily_details)
            self.realized_pnl_daily = self.realized_pnl_daily_df["realized_pnl"].sum() - self.realized_pnl_daily_df["cost"].sum()
        self.realized_pnl_cum += self.realized_pnl_daily
        return 0

    def update_nav(self) -> int:
        self.nav = self.init_cash + self.realized_pnl_cum + self.unrealized_pnl
        return 0

    def save_nav_snapshots(self) -> int:
        d = {
            "trade_date": self.update_date,
            "realized_pnl_daily": self.realized_pnl_daily,
            "realized_pnl_cum": self.realized_pnl_cum,
            "unrealized_pnl": self.unrealized_pnl,
            "nav": self.nav,
            "navps": self.nav / self.init_cash
        }
        self.nav_daily_snapshots.append(d)
        return 0

    def save_position(self) -> int:
        # format to DataFrame
        pos_data_list = []
        for pos in self.manager_pos.values():
            pos_data_list.append(pos.to_dict())
        pos_df = pd.DataFrame(pos_data_list)

        # save to csv
        if self.verbose:
            pos_file = f"{self.pid}.{self.update_date}.positions.csv.gz"
            pos_path = os.path.join(self.dir_pid_positions, pos_file)
            pos_df.to_csv(pos_path, index=False, float_format="%.6f", compression="gzip")
        return 0

    def save_trades(self) -> int:
        if self.verbose and (self.realized_pnl_daily_df is not None):
            records_trades_file = f"{self.pid}.{self.update_date}.trades.csv.gz"
            records_trades_path = os.path.join(self.dir_pid_trades, records_trades_file)
            self.realized_pnl_daily_df.to_csv(records_trades_path, index=False, float_format="%.6f", compression="gzip")
        return 0

    def save_nav(self) -> int:
        nav_daily_df = pd.DataFrame(self.nav_daily_snapshots)
        nav_daily_file = f"{self.pid}.nav.daily.csv.gz"
        nav_daily_path = os.path.join(self.dir_pid, nav_daily_file)
        nav_daily_df.to_csv(nav_daily_path, index=False, float_format="%.4f", compression="gzip")
        return 0

    def main_loop(self, simu_bgn_date: str, simu_stp_date: str, start_delay: int, hold_period_n: int,
                  trade_calendar: CCalendar,
                  mgr_signal: CManagerSignal, mgr_md: CManagerMarketData, mgr_major: CManagerMajor):
        iter_trade_dates_list = trade_calendar.get_iter_list(bgn_date=simu_bgn_date, stp_date=simu_stp_date)
        for ti, trade_date in enumerate(iter_trade_dates_list):
            # --- initialize
            signal_date = trade_calendar.get_next_date(trade_date, shift=-1)
            self.initialize_daily(trade_date=trade_date)

            # --- check signal and cal new positions
            if (ti - start_delay) % hold_period_n == 0:  # ti is an execution date
                new_pos_df = mgr_signal.cal_position_header(sig_date=signal_date)
                mgr_new_pos: dict[CPosKey, CPosition] = self.cal_target_position(new_pos_df=new_pos_df)
                array_new_trades = self.cal_trades_for_signal(mgr_new_pos=mgr_new_pos)
                # no major-shift check is necessary
                # because signal would contain this information itself already
            else:
                # array_new_trades = []
                array_new_trades = self.cal_trades_for_major(mgr_major=mgr_major)  # use this function to check for major-shift
            for new_trade in array_new_trades:
                contract_id, instrument = new_trade.contract_and_instru_id
                new_trade.executed_price = mgr_md.inquiry_price_at_date(contact=contract_id, instrument=instrument, trade_date=trade_date, price_type="close")
            self.update_from_trades(trades=array_new_trades)

            # --- update with market data for realized and unrealized pnl
            self.update_realized_pnl()
            self.update_unrealized_pnl(mgr_md=mgr_md)
            self.update_nav()

            # --- save snapshots
            self.save_trades()
            self.save_position()
            self.save_nav_snapshots()

        # save nav
        self.save_nav()
        return 0
