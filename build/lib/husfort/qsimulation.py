import os
import numpy as np
import pandas as pd
from husfort.qsqlite import CManagerLibReader, CLibFactor, CLibAvailableUniverse
from husfort.qcalendar import CCalendar
from husfort.qinstruments import (CInstrumentInfoTable, CPosKey, CContract,
                                  TOperation, CONST_DIRECTION_LNG, CONST_DIRECTION_SRT, CONST_OPERATION_OPN, CONST_OPERATION_CLS)


# ------------------------------------------ Classes general -------------------------------------------------------------------
class CManagerMajor(object):
    def __init__(self, universe: list[str], major_minor_dir: str, major_minor_lib_name: str = "major_minor.db"):
        src_db_reader = CManagerLibReader(major_minor_dir, major_minor_lib_name)
        self.m_major: dict[str, pd.DataFrame] = {}
        for instrument in universe:
            instrument_major_data_df = src_db_reader.read(
                value_columns=["trade_date", "n_contract"],
                using_default_table=False,
                table_name=instrument.replace(".", "_"))
            self.m_major[instrument] = instrument_major_data_df.set_index("trade_date")
        src_db_reader.close()

    def inquiry_major_contract(self, instrument: str, trade_date: str) -> str:
        return self.m_major[instrument].at[trade_date, "n_contract"]


class CManagerMarketData(object):
    def __init__(self, universe: list[str], market_data_dir: str, market_data_file_name_tmpl: str = "{}.md.{}.csv.gz",
                 price_types: tuple[str] = ("open", "close", "settle")):
        self.md: dict[str, dict[str, pd.DataFrame]] = {p: {} for p in price_types}
        for prc_type in self.md:
            for instrument_id in universe:
                instrument_md_file = market_data_file_name_tmpl.format(instrument_id, prc_type)
                instrument_md_path = os.path.join(market_data_dir, instrument_md_file)
                instrument_md_df = pd.read_csv(instrument_md_path, dtype={"trade_date": str}).set_index("trade_date")
                self.md[prc_type][instrument_id] = instrument_md_df

    def inquiry_price_at_date(self, contact: str, instrument: str, trade_date: str, price_type: str = "close") -> float:
        return self.md[price_type][instrument].at[trade_date, contact]


class CManagerSignal(object):
    def __init__(self, factor: str, universe: list[str], factors_dir: str, available_universe_dir: str,
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
        self.available_universe_lib: CManagerLibReader = CLibAvailableUniverse(lib_save_dir=available_universe_dir).get_lib_reader()
        self.mgr_md: CManagerMarketData = mgr_md
        self.mgr_major: CManagerMajor = mgr_major

    def close_libs(self):
        self.factor_lib.close()
        self.available_universe_lib.close()
        return 0

    def cal_position_header(self, sig_date: str) -> pd.DataFrame:
        # --- load factors at signal date
        factor_df = self.factor_lib.read_by_date(sig_date, value_columns=["instrument", "value"])
        factor_df = factor_df.rename(mapper={"value": self.factor}, axis=1).set_index("instrument")

        # --- load available universe
        au_df = self.available_universe_lib.read_by_date(sig_date, value_columns=["instrument"])
        au = au_df["instrument"].tolist()

        # --- selected/optimized universe
        header_universe = [_ for _ in self.universe if (_ in factor_df.index) and (_ in au)]
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

    def cal_quantity(self, price: float, allocated_mkt_val: float):
        self._quantity = int(np.round(allocated_mkt_val / price / self._pos_key.contract.contract_multiplier))
        return 0

    @property
    def pos_key(self) -> CPosKey:
        return self._pos_key

    @property
    def pos_id(self) -> tuple[str, str]:
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
    def unrealized_pnl(self) -> float:
        return (self.last_price - self.cost_price) * self.pos_key.direction * self.pos_key.contract.contract_multiplier * self._quantity

    def update_from_trade(self, exe_date: str, trade: CTrade, cost_rate: float) -> dict:
        quantity, executed_price = trade.executed_quantity, trade.executed_price
        cost = executed_price * quantity * self.pos_key.contract.contract_multiplier * cost_rate
        if trade.operation_is_opn():
            realized_pnl = 0
            amt_new = self.cost_price * self._quantity + executed_price * quantity
            self._quantity += quantity
            self.cost_price = amt_new / self._quantity
        elif trade.operation_is_cls():
            realized_pnl = (executed_price - self.cost_price) * self.pos_key.direction * self.pos_key.contract.contract_multiplier * quantity
            self._quantity -= quantity
        else:
            print(f"operation = {trade.operation} is illegal")
            raise ValueError
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
    def __init__(self, pid: str, init_cash: float, cost_reservation: float, cost_rate: float, dir_pid: str, verbose: bool = True):
        self.pid: str = pid

        # pnl
        self.init_cash: float = init_cash
        self.realized_pnl_cumsum: float = 0
        self.daily_unrealized_pnl: float = 0
        self.daily_realized_pnl: float = 0
        self.daily_summary: dict = {}
        self.nav: float = self.init_cash + self.realized_pnl_cumsum + self.daily_unrealized_pnl

        self.snapshots_nav = []
        self.snapshots_pos_dfs: list[pd.DataFrame] = []
        self.record_trades_dfs: list[pd.DataFrame] = []
        self.daily_snapshots_pos = []
        self.daily_record_trades = []

        # position
        self.manager_pos: dict[CPosKey, CPositionPlus] = {}

        # additional
        self.cost_reservation: float = cost_reservation
        self.cost_rate: float = cost_rate

        # save nav
        self.dir_pid: str = dir_pid
        self.verbose: bool = verbose

    def _initialize_daily(self, exe_date: str) -> int:
        self.daily_realized_pnl, self.daily_unrealized_pnl = 0.0, 0.0
        self.daily_record_trades.clear()
        self.daily_snapshots_pos.clear()
        self.daily_summary = {"trade_date": exe_date}
        return 0

    def _cal_target_position(self, new_pos_df: pd.DataFrame, instru_info_tab: CInstrumentInfoTable) -> dict[CPosKey, CPosition]:
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
        mgr_tgt_pos: dict[CPosKey, CPosition] = {}
        tot_allocated_amt = self.nav / (1 + self.cost_reservation)
        for contract_id, direction, price, weight in zip(new_pos_df["contract"], new_pos_df["direction"], new_pos_df["price"], new_pos_df["weight"]):
            if direction == 1:
                contract = CContract.gen_from_contract_id(contract_id, instru_info_tab)
                pos_key = CPosKey(contract, CONST_DIRECTION_LNG)
            elif direction == -1:
                contract = CContract.gen_from_contract_id(contract_id, instru_info_tab)
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
                mgr_tgt_pos[key] = tgt_pos
        return mgr_tgt_pos

    def _cal_trades_for_signal(self, mgr_tgt_pos: dict[CPosKey, CPosition]) -> list[CTrade]:
        trades: list[CTrade] = []
        # cross comparison: step 0, check if new position is in old position
        for new_key, new_pos in mgr_tgt_pos.items():
            if new_key in self.manager_pos:
                new_trade: CTrade = self.manager_pos[new_key].cal_trade_from_other_pos(other=new_pos)  # could be none
            else:
                self.manager_pos[new_key] = CPositionPlus(pos_key=new_key)
                new_trade: CTrade = new_pos.get_open_trade()
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
            (old_contract_id, instrument), quantity = old_pos.pos_id, old_pos.quantity
            new_contract_id = mgr_major.inquiry_major_contract(instrument=instrument, trade_date=sig_date)
            if old_contract_id != new_contract_id:
                # close old
                trades.append(old_pos.get_close_trade())
                # open  new
                new_contract = CContract.gen_from_other(new_contract_id, old_key.contract)
                pos_key = CPosKey(new_contract, old_key.direction)
                trade_open_new = CTrade(pos_key=pos_key, operation=CONST_OPERATION_OPN, quantity=quantity)
                trades.append(trade_open_new)
        return trades

    def _update_from_trades(self, trades: list[CTrade], exe_date: str):
        for trade in trades:
            trade_result = self.manager_pos[trade.pos_key].update_from_trade(exe_date, trade, self.cost_rate)
            self.daily_record_trades.append(trade_result)

        for pos_key in list(self.manager_pos):  # remove empty positions
            if self.manager_pos[pos_key].is_empty:
                del self.manager_pos[pos_key]
        return 0

    def _update_positions(self, mgr_md: CManagerMarketData, price_type: str, exe_date: str) -> int:
        for pos in self.manager_pos.values():
            contract, instrument = pos.pos_id
            last_price = mgr_md.inquiry_price_at_date(
                contact=contract, instrument=instrument, trade_date=exe_date,
                price_type=price_type
            )
            if np.isnan(last_price):
                print(f"nan price for {contract} {exe_date}")
            else:
                pos.update_last_price(price=last_price)
            self.daily_snapshots_pos.append(pos.to_dict(exe_date))
        return 0

    @staticmethod
    def _get_pos_summary(df: pd.DataFrame) -> dict:
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

    def _cal_unrealized_pnl(self):
        pos_daily_df = pd.DataFrame(self.daily_snapshots_pos)
        if not pos_daily_df.empty:
            self.daily_unrealized_pnl = pos_daily_df["unrealized_pnl"].sum()
            self.snapshots_pos_dfs.append(pos_daily_df)
        self.daily_summary.update(self._get_pos_summary(pos_daily_df))
        return 0

    def _cal_realized_pnl(self):
        realized_pnl_daily_df = pd.DataFrame(self.daily_record_trades)
        if not realized_pnl_daily_df.empty:
            self.daily_realized_pnl = realized_pnl_daily_df["realized_pnl"].sum() - realized_pnl_daily_df["cost"].sum()
            self.record_trades_dfs.append(realized_pnl_daily_df)
        self.realized_pnl_cumsum += self.daily_realized_pnl
        self.daily_summary.update({
            "realizedPnl": self.daily_realized_pnl,
            "realizedPnlCumSum": self.realized_pnl_cumsum,
        })
        return 0

    def _cal_nav(self) -> int:
        self.nav = self.init_cash + self.realized_pnl_cumsum + self.daily_unrealized_pnl
        self.daily_summary.update({
            "nav": self.nav,
            "navps": self.nav / self.init_cash
        })
        self.snapshots_nav.append(self.daily_summary)
        return 0

    def _save_position(self) -> int:
        if self.verbose:
            positions_df = pd.concat(self.snapshots_pos_dfs, axis=0, ignore_index=True)
            positions_file = f"{self.pid}.positions.csv.gz"
            positions_path = os.path.join(self.dir_pid, positions_file)
            positions_df.to_csv(positions_path, index=False, float_format="%.6f", compression="gzip")
        return 0

    def _save_trades(self) -> int:
        if self.verbose:
            record_trades_df = pd.concat(self.record_trades_dfs, axis=0, ignore_index=True)
            record_trades_file = f"{self.pid}.trades.csv.gz"
            record_trades_path = os.path.join(self.dir_pid, record_trades_file)
            record_trades_df.to_csv(record_trades_path, index=False, float_format="%.6f", compression="gzip")
        return 0

    def _save_nav(self) -> int:
        nav_daily_df = pd.DataFrame(self.snapshots_nav)
        nav_daily_file = f"{self.pid}.nav.daily.csv.gz"
        nav_daily_path = os.path.join(self.dir_pid, nav_daily_file)
        nav_daily_df.to_csv(nav_daily_path, index=False, float_format="%.4f", compression="gzip")
        return 0

    def main(self, simu_bgn_date: str, simu_stp_date: str, start_delay: int, hold_period_n: int,
             calendar: CCalendar, instru_info_tab: CInstrumentInfoTable,
             mgr_signal: CManagerSignal, mgr_md: CManagerMarketData, mgr_major: CManagerMajor):
        base_date = calendar.get_next_date(simu_bgn_date, -1)
        trade_dates = calendar.get_iter_list(bgn_date=base_date, stp_date=simu_stp_date)
        for ti, exe_date in enumerate(trade_dates[1:]):
            # --- initialize
            sig_date = trade_dates[ti]
            self._initialize_daily(exe_date=exe_date)

            # --- check signal and major shift to create new trades ---
            if (ti - start_delay) % hold_period_n == 0:  # ti is an execution date
                # no major-shift check is necessary
                # because signal would contain this information itself already
                new_pos_df = mgr_signal.cal_position_header(sig_date=sig_date)
                mgr_new_pos: dict[CPosKey, CPosition] = self._cal_target_position(new_pos_df, instru_info_tab)
                new_trades = self._cal_trades_for_signal(mgr_tgt_pos=mgr_new_pos)
            else:
                new_trades = self._cal_trades_for_major(mgr_major=mgr_major, sig_date=sig_date)  # use this function to check for major-shift

            # --- set price for new trade
            for new_trade in new_trades:
                contract_id, instrument = new_trade.contract_and_instru_id()
                new_trade.executed_price = mgr_md.inquiry_price_at_date(contract_id, instrument, exe_date)

            # --- update from trades and position
            self._update_from_trades(trades=new_trades, exe_date=exe_date)
            self._update_positions(mgr_md=mgr_md, price_type="close", exe_date=exe_date)

            # --- update with market data for realized and unrealized pnl
            self._cal_realized_pnl()
            self._cal_unrealized_pnl()
            self._cal_nav()

        # save nav
        self._save_nav()
        self._save_trades()
        self._save_position()
        return 0


if __name__ == "__main__":
    test_universe = concerned_instruments_universe = [
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
    test_bgn_date, test_stp_date = "20140701", "20240109"
    test_calendar = CCalendar(r"E:\Deploy\Data\Calendar\cne_calendar.csv")
    test_instru_info_tab = CInstrumentInfoTable(r"E:\Deploy\Data\Futures\InstrumentInfo3.csv", file_type="CSV", index_label="windCode")
    test_mgr_md = CManagerMarketData(universe=test_universe, market_data_dir=r"E:\Deploy\Data\Futures\by_instrument\by_instru_md")
    test_mgr_major = CManagerMajor(universe=test_universe, major_minor_dir=r"E:\Deploy\Data\Futures\by_instrument")
    test_mgr_signal = CManagerSignal(factor="ND", universe=test_universe,
                                     factors_dir=r"E:\Deploy\Data\ForProjects\cta3\signals\portfolios",
                                     available_universe_dir=r"E:\Deploy\Data\ForProjects\cta3\available_universe",
                                     mgr_md=test_mgr_md, mgr_major=test_mgr_major)

    test_portfolio = CPortfolio(
        pid="test_simu", init_cash=10000000,
        cost_reservation=0, cost_rate=5e-4,
        dir_pid=r"E:\TMP"
    )
    test_portfolio.main(
        simu_bgn_date=test_bgn_date, simu_stp_date=test_stp_date, start_delay=0, hold_period_n=1,
        calendar=test_calendar, instru_info_tab=test_instru_info_tab,
        mgr_signal=test_mgr_signal, mgr_md=test_mgr_md, mgr_major=test_mgr_major,
    )
