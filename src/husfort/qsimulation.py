import os
import multiprocessing as mp
import numpy as np
import pandas as pd
from rich.progress import track
from loguru import logger
from husfort.qutility import check_and_mkdir, qtimer
from husfort.qsqlite import CLibFactor, CLibAvailableUniverse
from husfort.qcalendar import CCalendar
from husfort.qinstruments import (CInstrumentInfoTable, CPosKey, CContract,
                                  TOperation, TDirection,
                                  CONST_DIRECTION_LNG, CONST_DIRECTION_SRT,
                                  CONST_OPERATION_OPN, CONST_OPERATION_CLS)
from husfort import CManagerMarketData, CManagerMajor


class CSignal(object):
    def __init__(self, contract: CContract, direction: TDirection, price: float, weight: float):
        self.contract = contract
        self.direction = direction
        self.price = price
        self.weight = weight


class CManagerSignal(object):
    def __init__(self, factor: str, universe: list[str], factors_dir: str, available_universe_dir: str):
        """

        :param factor:
        :param universe: list of all instruments, instrument not in this list can not be traded
        :param factors_dir:
        """

        self.factor = factor
        self.universe: set = set(universe)
        self.__init__factor(factor=factor, factors_dir=factors_dir)
        self.__init_available_universe(available_universe_dir=available_universe_dir)

    def __init__factor(self, factor: str, factors_dir):
        lib_reader = CLibFactor(factor=factor, lib_save_dir=factors_dir).get_lib_reader()
        df = lib_reader.read(value_columns=["trade_date", "instrument", "value"])
        df.rename(mapper={"value": self.factor}, axis=1, inplace=True)
        lib_reader.close()
        self.factor_lib: dict[str, pd.DataFrame] = {}
        for (trade_date, trade_date_df) in df.groupby(by="trade_date"):
            self.factor_lib[trade_date] = trade_date_df.drop(axis=1, labels=["trade_date"]).set_index("instrument")
        return 0

    def __init_available_universe(self, available_universe_dir: str):
        lib_reader = CLibAvailableUniverse(available_universe_dir).get_lib_reader()
        df = lib_reader.read(value_columns=["trade_date", "instrument"])
        lib_reader.close()
        self.available_universe_lib: dict[str, list[str]] = {}
        for (trade_date, trade_date_df) in df.groupby(by="trade_date"):
            self.available_universe_lib[trade_date] = trade_date_df["instrument"].to_list()
        return 0

    def inquire_factor(self, trade_date: str) -> pd.DataFrame:
        df = self.factor_lib.get(
            trade_date,
            pd.DataFrame(data={"factor": []}, index=pd.Index(name="instrument", data=[]))
        )
        return df

    def inquire_available_universe(self, trade_date: str) -> list[str]:
        return self.available_universe_lib.get(trade_date, [])

    def cal_signals(self, sig_date: str, mgr_md: CManagerMarketData, mgr_major: CManagerMajor,
                    instru_info_tab: CInstrumentInfoTable) -> list[CSignal]:
        # --- load factor and available universe
        factor_df = self.inquire_factor(sig_date)
        au = self.inquire_available_universe(sig_date)

        # --- signals
        selected_universe = [_ for _ in self.universe if (_ in factor_df.index) and (_ in au)]
        signal_df: pd.DataFrame = factor_df.loc[selected_universe]
        if signal_df.empty:
            return []

        signals: list[CSignal] = []
        for r in signal_df.itertuples():
            instrument, w = getattr(r, "Index"), getattr(r, self.factor)
            contract_id = mgr_major.inquiry_major_contract(instrument, sig_date)
            price = mgr_md.inquiry_price_at_date(contract_id, instrument, sig_date)
            if w > 0:
                contract = CContract.gen_from_contract_id(contract_id, instru_info_tab)
                signals.append(CSignal(contract, CONST_DIRECTION_LNG, price=price, weight=w))
            elif w < 0:
                contract = CContract.gen_from_contract_id(contract_id, instru_info_tab)
                signals.append(CSignal(contract, CONST_DIRECTION_SRT, price=price, weight=-w))
            else:  # w == 0
                pass
        return signals


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
    def operation(self) -> TOperation:
        return self._operation

    @property
    def executed_quantity(self) -> int:
        return self._quantity

    @property
    def executed_price(self) -> float:
        return self._executed_price

    @executed_price.setter
    def executed_price(self, executed_price: float):
        self._executed_price = executed_price

    def contract_and_instru_id(self) -> tuple[str, str]:
        return self._pos_key.contract.contract_and_instru_id()

    def operation_is_opn(self) -> bool:
        return self._operation == CONST_OPERATION_OPN

    def operation_is_cls(self) -> bool:
        return self._operation == CONST_OPERATION_CLS


# --- Class: Position
class CPosition(object):
    def __init__(self, pos_key: CPosKey):
        self._pos_key: CPosKey = pos_key
        self._quantity: int = 0

    def cal_quantity(self, price: float, money_amt: float):
        self._quantity = int(np.round(money_amt / price / self._pos_key.contract.contract_multiplier))
        return 0

    @property
    def pos_key(self) -> CPosKey:
        return self._pos_key

    @property
    def contract_and_instru_id(self) -> tuple[str, str]:
        return self.pos_key.contract.contract_and_instru_id()

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

    def get_open_trade(self) -> CTrade:
        new_trade: CTrade = CTrade(pos_key=self.pos_key, operation=CONST_OPERATION_OPN, quantity=self.quantity)
        return new_trade

    def get_close_trade(self) -> CTrade:
        new_trade: CTrade = CTrade(pos_key=self.pos_key, operation=CONST_OPERATION_CLS, quantity=self.quantity)
        return new_trade

    def to_dict(self, trade_date: str) -> dict:
        return {
            "trade_date": trade_date,
            "contact": self.pos_key.contract.contract,
            "direction": self.pos_key.direction,
            "quantity": self.quantity,
            "contract_multiplier": self.pos_key.contract.contract_multiplier,
        }


# --- Class: PositionPlus
class CPositionPlus(CPosition):
    def __init__(self, pos_key: CPosKey):
        super().__init__(pos_key=pos_key)
        self.cost_price: float = 0
        self.last_price: float = 0

    @property
    def last_value(self) -> float:
        return self.last_price * self.pos_key.direction * self.pos_key.contract.contract_multiplier * self._quantity

    @property
    def cost_value(self) -> float:
        return self.cost_price * self.pos_key.direction * self.pos_key.contract.contract_multiplier * self._quantity

    @property
    def unrealized_pnl(self) -> float:
        dp = self.last_price - self.cost_price
        return dp * self.pos_key.direction * self.pos_key.contract.contract_multiplier * self._quantity

    def update_from_trade(self, exe_date: str, trade: CTrade, cost_rate: float) -> dict:
        quantity, executed_price = trade.executed_quantity, trade.executed_price
        cost = executed_price * quantity * self.pos_key.contract.contract_multiplier * cost_rate
        if trade.operation_is_opn():
            realized_pnl = 0
            amt_new = self.cost_price * self._quantity + executed_price * quantity
            self._quantity += quantity
            self.cost_price = amt_new / self._quantity
        elif trade.operation_is_cls():
            dp = executed_price - self.cost_price
            realized_pnl = dp * self.pos_key.direction * self.pos_key.contract.contract_multiplier * quantity
            self._quantity -= quantity
        else:
            raise ValueError(f"operation = {trade.operation} is illegal")
        return {
            "trade_date": exe_date,
            "contract": self.pos_key.contract.contract,
            "direction": self.pos_key.direction,
            "operation": trade.operation,
            "quantity": quantity,
            "price": executed_price,
            "cost": cost,
            "realized_pnl": realized_pnl,
        }

    def update_last_price(self, price: float):
        self.last_price = price
        return 0

    def to_dict(self, trade_date: str) -> dict:
        d = super().to_dict(trade_date)
        d.update(
            {
                "cost_price": self.cost_price,
                "last_price": self.last_price,
                "last_value": self.last_value,
                "unrealized_pnl": self.unrealized_pnl,
            }
        )
        return d


# --- Class: Portfolio
class CPortfolio(object):
    class _CDailyRecorder(object):
        def __init__(self):
            self.unrealized_pnl: float = 0.0
            self.realized_pnl: float = 0.0
            self.summary: dict = {}
            self.snapshots_pos: list[dict] = []
            self.record_trades: list[dict] = []

            self.snapshots_pos_df = pd.DataFrame()
            self.record_trades_df = pd.DataFrame()

        def reset(self, exe_date: str):
            self.unrealized_pnl, self.realized_pnl = 0.0, 0.0
            self.snapshots_pos.clear()
            self.record_trades.clear()
            self.snapshots_pos_df = pd.DataFrame()
            self.record_trades_df = pd.DataFrame()
            self.summary = {"trade_date": exe_date}
            return 0

        def _get_pos_summary(self) -> dict:
            df = self.snapshots_pos_df
            if df.empty:
                return {
                    "qtyPos": 0,
                    "qtyNeg": 0,
                    "qtyTot": 0,
                    "valPos": 0,
                    "valNeg": 0,
                    "valNet": 0,
                    "valTot": 0,
                    "unrealizedPnl": 0,
                }
            else:
                filter_pos = df["direction"] > 0
                filter_neg = df["direction"] < 0
                pos_df, neg_df = df.loc[filter_pos], df.loc[filter_neg]
                qty_pos, qty_neg = pos_df["quantity"].sum(), neg_df["quantity"].sum()
                qty_tot = qty_pos + qty_neg
                val_pos, val_neg = pos_df["last_value"].sum(), neg_df["last_value"].sum()
                val_net, val_tot = val_pos + val_neg, val_pos - val_neg
                unrealized_pnl = df["unrealized_pnl"].sum()
                return {
                    "qtyPos": qty_pos,
                    "qtyNeg": qty_neg,
                    "qtyTot": qty_tot,
                    "valPos": val_pos,
                    "valNeg": val_neg,
                    "valNet": val_net,
                    "valTot": val_tot,
                    "unrealizedPnl": unrealized_pnl,
                }

        def update_unrealized(self):
            if self.snapshots_pos:
                self.snapshots_pos_df = pd.DataFrame(self.snapshots_pos)
                self.unrealized_pnl = self.snapshots_pos_df["unrealized_pnl"].sum()
            self.summary.update(self._get_pos_summary())
            return 0

        def get_snapshots_pos(self) -> pd.DataFrame:
            return self.snapshots_pos_df

        def update_realized(self):
            if self.record_trades:
                self.record_trades_df = pd.DataFrame(self.record_trades)
                sum_realized_pnl = self.record_trades_df["realized_pnl"].sum()
                sum_cost = self.record_trades_df["cost"].sum()
                self.realized_pnl = sum_realized_pnl - sum_cost
            self.summary.update({"realizedPnl": self.realized_pnl})
            return 0

        def get_record_trades(self) -> pd.DataFrame:
            return self.record_trades_df

        def update_nav(self, realized_pnl_cumsum: float, nav: float, navps: float):
            self.summary.update({
                "realizedPnlCumSum": realized_pnl_cumsum,
                "nav": nav,
                "navps": navps,
            })
            return 0

    class _CSimuRecorder(object):
        def __init__(self, init_cash: float):
            self.init_cash: float = init_cash
            self.realized_pnl_cumsum: float = 0.0
            self.nav: float = 0.0
            self.navps: float = 0.0
            self.update_nav(unrealized_pnl=0.0, realized_pnl=0.0)
            self.snapshots_nav = []
            self.snapshots_pos_dfs: list[pd.DataFrame] = []
            self.record_trades_dfs: list[pd.DataFrame] = []

        def update_nav(self, unrealized_pnl: float, realized_pnl: float):
            self.realized_pnl_cumsum += realized_pnl
            self.nav = self.init_cash + self.realized_pnl_cumsum + unrealized_pnl
            self.navps = self.nav / self.init_cash
            return 0

        def take_snapshots_of_nav(self, d: dict):
            self.snapshots_nav.append(d)
            return 0

    def __init__(self, pid: str, init_cash: float, cost_reservation: float, cost_rate: float,
                 dir_pid: str, save_trades_and_positions: bool = True,
                 record_trades_file_tmpl: str = "{}.trades.csv.gz",
                 snapshots_pos_file_tmpl: str = "{}.positions.csv.gz",
                 nav_daily_file_tmpl: str = "{}.nav_daily.csv.gz",
                 ):
        self.pid: str = pid
        self.daily_recorder = self._CDailyRecorder()
        self.simu_recorder = self._CSimuRecorder(init_cash)

        # position
        self.manager_pos: dict[CPosKey, CPositionPlus] = {}

        # additional
        self.cost_reservation: float = cost_reservation
        self.cost_rate: float = cost_rate

        # save nav
        self.dir_pid: str = dir_pid
        self.save_trades_and_positions: bool = save_trades_and_positions
        self.record_trades_file_tmpl = record_trades_file_tmpl
        self.snapshots_pos_file_tmpl = snapshots_pos_file_tmpl
        self.nav_daily_file_tmpl = nav_daily_file_tmpl

    def _initialize_daily(self, exe_date: str) -> int:
        self.daily_recorder.reset(exe_date)
        return 0

    def _cal_target_position(self, signals: list[CSignal]) -> dict[CPosKey, CPosition]:
        """

        :param signals : a list of CSignal
                        a.1 this "price" is used to estimate how much quantity should be allocated
                            for the instrument, CLOSE-PRICE is most frequently used, but other types
                            such as OPEN could do the job as well. if new position is to open with T
                            day's price, this "price" should be from T-1, which is available.
                        a.2 direction: 1 for long, -1 for short.
                        a.3 weight: non-negative value, sum of weights should not be greater than 1, if
                            leverage are not allowed
        :return:
        """
        mgr_tgt_pos: dict[CPosKey, CPosition] = {}
        tot_allocated_amt = self.simu_recorder.nav / (1 + self.cost_reservation)
        for signal in signals:
            pos_key = CPosKey(signal.contract, signal.direction)
            tgt_pos = CPosition(pos_key)
            tgt_pos.cal_quantity(price=signal.price, money_amt=tot_allocated_amt * signal.weight)
            if tgt_pos.quantity > 0:
                mgr_tgt_pos[pos_key] = tgt_pos
        return mgr_tgt_pos

    def _cal_trades_for_signal(self, mgr_tgt_pos: dict[CPosKey, CPosition]) -> list[CTrade]:
        trades: list[CTrade] = []
        # cross comparison: step 0, check if new position is in old position
        for new_key, new_pos in mgr_tgt_pos.items():
            if new_key not in self.manager_pos:
                self.manager_pos[new_key] = CPositionPlus(new_key)
            new_trade: CTrade = self.manager_pos[new_key].cal_trade_from_other_pos(other=new_pos)  # could be none
            if new_trade is not None:
                trades.append(new_trade)

        # cross comparison: step 1, check if old position is in new position
        for old_key, old_pos in self.manager_pos.items():
            if old_key not in mgr_tgt_pos:
                new_trade: CTrade = old_pos.get_close_trade()
                trades.append(new_trade)
        return trades

    def _cal_trades_for_major(self, mgr_major: CManagerMajor, sig_date: str) -> list[CTrade]:
        trades: list[CTrade] = []
        for old_key, old_pos in self.manager_pos.items():
            (old_contract_id, instrument), quantity = old_pos.contract_and_instru_id, old_pos.quantity
            new_contract_id = mgr_major.inquiry_major_contract(instrument=instrument, trade_date=sig_date)
            if old_contract_id != new_contract_id:
                trades.append(old_pos.get_close_trade())  # close old
                new_key = CPosKey(contract=CContract.gen_from_other(new_contract_id, old_key.contract),
                                  direction=old_key.direction)
                self.manager_pos[new_key] = CPositionPlus(new_key)
                trades.append(CTrade(new_key, CONST_OPERATION_OPN, quantity))  # open  new
        return trades

    @staticmethod
    def _match_price_for_trades(trades: list[CTrade],
                                mgr_md: CManagerMarketData, exe_date: str, trade_price_type: str):
        for trade in trades:
            contract_id, instrument = trade.contract_and_instru_id()
            trade.executed_price = mgr_md.inquiry_price_at_date(contract_id, instrument, exe_date, trade_price_type)
        return 0

    def _update_from_trades(self, trades: list[CTrade], exe_date: str):
        for trade in trades:
            trade_result = self.manager_pos[trade.pos_key].update_from_trade(exe_date, trade, self.cost_rate)
            self.daily_recorder.record_trades.append(trade_result)
        for pos_key in list(self.manager_pos):  # remove empty positions
            if self.manager_pos[pos_key].is_empty:
                del self.manager_pos[pos_key]
        return 0

    def _update_positions(self, mgr_md: CManagerMarketData, settle_price_type: str, exe_date: str) -> int:
        for pos in self.manager_pos.values():
            contract_id, instrument = pos.contract_and_instru_id
            last_price = mgr_md.inquiry_price_at_date(
                contact=contract_id, instrument=instrument, trade_date=exe_date,
                price_type=settle_price_type
            )
            if np.isnan(last_price):
                logger.info(f"nan price for {contract_id} @ {exe_date}")
            else:
                pos.update_last_price(price=last_price)
            self.daily_recorder.snapshots_pos.append(pos.to_dict(exe_date))
        return 0

    def _cal_unrealized_pnl(self):
        self.daily_recorder.update_unrealized()
        snapshots_pos_df = self.daily_recorder.get_snapshots_pos()
        if not snapshots_pos_df.empty:
            self.simu_recorder.snapshots_pos_dfs.append(snapshots_pos_df)
        return 0

    def _cal_realized_pnl(self):
        self.daily_recorder.update_realized()
        record_trades_df = self.daily_recorder.get_record_trades()
        if not record_trades_df.empty:
            self.simu_recorder.record_trades_dfs.append(record_trades_df)
        return 0

    def _cal_nav(self) -> int:
        self.simu_recorder.update_nav(
            unrealized_pnl=self.daily_recorder.unrealized_pnl,
            realized_pnl=self.daily_recorder.realized_pnl,
        )
        self.daily_recorder.update_nav(
            realized_pnl_cumsum=self.simu_recorder.realized_pnl_cumsum,
            nav=self.simu_recorder.nav,
            navps=self.simu_recorder.navps,
        )
        self.simu_recorder.take_snapshots_of_nav(self.daily_recorder.summary)
        return 0

    def _save_position(self) -> int:
        if self.save_trades_and_positions:
            positions_df = pd.concat(self.simu_recorder.snapshots_pos_dfs, axis=0, ignore_index=True)
            positions_file = self.snapshots_pos_file_tmpl.format(self.pid)
            positions_path = os.path.join(self.dir_pid, positions_file)
            positions_df.to_csv(positions_path, index=False, float_format="%.6f", compression="gzip")
        return 0

    def _save_trades(self) -> int:
        if self.save_trades_and_positions:
            record_trades_df = pd.concat(self.simu_recorder.record_trades_dfs, axis=0, ignore_index=True)
            record_trades_file = self.record_trades_file_tmpl.format(self.pid)
            record_trades_path = os.path.join(self.dir_pid, record_trades_file)
            record_trades_df.to_csv(record_trades_path, index=False, float_format="%.6f", compression="gzip")
        return 0

    def _save_nav(self) -> int:
        nav_daily_df = pd.DataFrame(self.simu_recorder.snapshots_nav)
        nav_daily_df["navps"] = nav_daily_df["navps"].map(lambda _: f"{_:.6f}")
        nav_daily_file = self.nav_daily_file_tmpl.format(self.pid)
        nav_daily_path = os.path.join(self.dir_pid, nav_daily_file)
        nav_daily_df.to_csv(nav_daily_path, index=False, float_format="%.2f", compression="gzip")
        return 0

    def main(
            self, simu_bgn_date: str, simu_stp_date: str,
            calendar: CCalendar, instru_info_tab: CInstrumentInfoTable,
            mgr_signal: CManagerSignal, mgr_md: CManagerMarketData, mgr_major: CManagerMajor,
            trade_price_type: str = "close", settle_price_type: str = "close"
    ):
        exe_dates = calendar.get_iter_list(bgn_date=simu_bgn_date, stp_date=simu_stp_date)
        base_date = calendar.get_next_date(exe_dates[0], -1)
        sig_dates = [base_date] + exe_dates[:-1]
        iter_dates = list(zip(enumerate(exe_dates), sig_dates))
        for (ti, exe_date), sig_date in track(iter_dates, description=f"Complex Simulation for {self.pid}"):
            # --- initialize
            self._initialize_daily(exe_date=exe_date)

            # --- check signal and major shift to create new trades ---
            # no major-shift check is necessary
            # because signal would contain this information itself already
            new_pos_df = mgr_signal.cal_signals(sig_date, mgr_md, mgr_major, instru_info_tab)
            mgr_new_pos: dict[CPosKey, CPosition] = self._cal_target_position(new_pos_df)
            new_trades = self._cal_trades_for_signal(mgr_tgt_pos=mgr_new_pos)

            # --- set price for new trade
            self._match_price_for_trades(new_trades, mgr_md, exe_date, trade_price_type)

            # --- update from trades and position
            self._update_from_trades(trades=new_trades, exe_date=exe_date)
            self._update_positions(mgr_md=mgr_md, settle_price_type=settle_price_type, exe_date=exe_date)

            # --- update with market data for realized and unrealized pnl
            self._cal_realized_pnl()
            self._cal_unrealized_pnl()
            self._cal_nav()

        # save nav
        self._save_nav()
        self._save_trades()
        self._save_position()
        return 0


@qtimer
def cal_multiple_complex_simulations(
        signal_ids: list[str],
        universe: list[str],
        init_cash: float,
        cost_rate: float,
        simu_bgn_date: str,
        simu_stp_date: str,
        signals_dir: str,
        simulations_save_dir: str,
        calendar_path: str,
        instru_info_path: str,
        market_data_dir: str,
        major_minor_dir: str,
        available_universe_dir: str,
        call_multiprocess: bool,
        proc_qty: int = None,
        cost_reservation: float = 0,
        trade_price_type: str = "close",
        settle_price_type: str = "close",
        save_trades_and_positions: bool = False,
):
    # shared
    calendar = CCalendar(calendar_path)
    instru_info_tab = CInstrumentInfoTable(instru_info_path=instru_info_path, file_type="CSV", index_label="windCode")
    mgr_md = CManagerMarketData(universe=universe, market_data_dir=market_data_dir)
    mgr_major = CManagerMajor(universe=universe, major_minor_dir=major_minor_dir)
    logger.info(f"prerequisite loaded ...")

    # Serialize
    signals: list[tuple[CPortfolio, CManagerSignal]] = []
    for signal_id in signal_ids:
        pid = f"{signal_id}"
        dir_pid = os.path.join(simulations_save_dir, pid)
        check_and_mkdir(dir_pid)
        p = CPortfolio(
            pid=pid,
            init_cash=init_cash,
            cost_reservation=cost_reservation,
            cost_rate=cost_rate,
            dir_pid=dir_pid,
            save_trades_and_positions=save_trades_and_positions,
        )
        mgr_signal = CManagerSignal(
            factor=signal_id,
            universe=universe,
            factors_dir=signals_dir,
            available_universe_dir=available_universe_dir,
        )
        signals.append((p, mgr_signal))

    if call_multiprocess:
        pool = mp.Pool(processes=proc_qty)
        for p, mgr_signal in signals:
            pool.apply_async(
                p.main,
                kwds={
                    "simu_bgn_date": simu_bgn_date,
                    "simu_stp_date": simu_stp_date,
                    "calendar": calendar,
                    "instru_info_tab": instru_info_tab,
                    "mgr_signal": mgr_signal,
                    "mgr_md": mgr_md,
                    "mgr_major": mgr_major,
                    "trade_price_type": trade_price_type,
                    "settle_price_type": settle_price_type,
                },
            )
        pool.close()
        pool.join()
    else:
        for p, mgr_signal in signals:
            p.main(
                simu_bgn_date=simu_bgn_date,
                simu_stp_date=simu_stp_date,
                calendar=calendar,
                instru_info_tab=instru_info_tab,
                mgr_signal=mgr_signal,
                mgr_md=mgr_md,
                mgr_major=mgr_major,
                trade_price_type=trade_price_type,
                settle_price_type=settle_price_type,
            )
    return 0


if __name__ == "__main__":
    test_universe = [
        "AU.SHF",
        "AG.SHF",
        "CU.SHF",
        "AL.SHF",
        "PB.SHF",
        "ZN.SHF",
        "SN.SHF",
        "NI.SHF",
        "SS.SHF",
        "RB.SHF",
        "HC.SHF",
        "J.DCE",
        "JM.DCE",
        "I.DCE",
        "FG.CZC",
        "SA.CZC",
        "UR.CZC",
        "ZC.CZC",
        "SF.CZC",
        "SM.CZC",
        "Y.DCE",
        "P.DCE",
        "OI.CZC",
        "M.DCE",
        "RM.CZC",
        "A.DCE",
        "RU.SHF",
        "BU.SHF",
        "FU.SHF",
        "L.DCE",
        "V.DCE",
        "PP.DCE",
        "EG.DCE",
        "EB.DCE",
        "PG.DCE",
        "TA.CZC",
        "MA.CZC",
        "SP.SHF",
        "CF.CZC",
        "CY.CZC",
        "SR.CZC",
        "C.DCE",
        "CS.DCE",
        "JD.DCE",
        "LH.DCE",
        "AP.CZC",
        "CJ.CZC",
    ]
    test_bgn_date, test_stp_date = "20140701", "20240226"
    t_signals_dir = r"E:\Deploy\Data\ForProjects\cta3\signals\portfolios"
    t_simulations_save_dir = r"E:\TMP"
    t_calendar_path = r"E:\Deploy\Data\Calendar\cne_calendar.csv"
    t_instru_info_path = r"E:\Deploy\Data\Futures\InstrumentInfo3.csv"
    t_market_data_dir = r"E:\Deploy\Data\Futures\by_instrument\by_instru_md"
    t_major_minor_dir = r"E:\Deploy\Data\Futures\by_instrument"
    t_available_universe_dir = r"E:\Deploy\Data\ForProjects\cta3\available_universe"

    cal_multiple_complex_simulations(
        signal_ids=["ND", "NF"],
        universe=test_universe,
        init_cash=10000000,
        cost_rate=5e-4,
        simu_bgn_date=test_bgn_date,
        simu_stp_date=test_stp_date,
        signals_dir=t_signals_dir,
        simulations_save_dir=t_simulations_save_dir,
        calendar_path=t_calendar_path,
        instru_info_path=t_instru_info_path,
        market_data_dir=t_market_data_dir,
        major_minor_dir=t_major_minor_dir,
        available_universe_dir=t_available_universe_dir,
        call_multiprocess=True,
    )
