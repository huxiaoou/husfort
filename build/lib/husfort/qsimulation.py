import os
import numpy as np
import pandas as pd
from husfort.qsqlite import CManagerLibReader, CLibAvailableUniverse, CLibFactor


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


class CManagerSignalBase(object):
    def __init__(self, factor: str, mother_universe: list[str], available_universe_dir: str, factors_dir: str,
                 mgr_md: CManagerMarketData, mgr_major: CManagerMajor, verbose: bool = True):
        """

        :param factor:
        :param mother_universe: list of all instruments, instrument not in this list can not be traded
        :param available_universe_dir:
        :param factors_dir:
        :param mgr_md:
        :param mgr_major:
        :param verbose:

        """

        self.factor = factor
        self.mother_universe_set: set = set(mother_universe)
        self.available_universe_lib: CManagerLibReader = CLibAvailableUniverse(available_universe_dir).get_lib_reader()
        self.factor_lib: CManagerLibReader = CLibFactor(factor=factor, lib_save_dir=factors_dir).get_lib_reader()
        self.mgr_md: CManagerMarketData = mgr_md
        self.mgr_major: CManagerMajor = mgr_major

    def close_libs(self):
        self.available_universe_lib.close()
        self.factor_lib.close()
        return 0

    @staticmethod
    def cal_weight(opt_weight_df: pd.DataFrame, weight_lbl:str):
        """

        :param opt_weight_df: factor in opt_weight_df is the weight, each element of
                              can be negative or positive. the sum of the absolute value of
                              each element may not equal 1.
        :return:
        """
        wgt_abs_sum = opt_weight_df[weight_lbl].abs().sum()
        opt_weight_df[weight_lbl] = opt_weight_df[weight_lbl] / wgt_abs_sum if wgt_abs_sum > 1e-4 else 0
        return opt_weight_df

    def cal_new_pos(self, sig_date: str, exe_date: str) -> pd.DataFrame:
        """

        :param sig_date:
        :param exe_date:
        :return:
        """

        # --- load available universe
        available_universe_df = self.available_universe_lib.read_by_date(sig_date, value_columns=["instrument"])
        available_universe_set = set(available_universe_df["instrument"])

        # --- load factors at signal date
        factor_df = self.factor_lib.read_by_date(sig_date, value_columns=["instrument", "value"]).rename(
            mapper={"value": self.factor}, axis=1).set_index("instrument")
        factor_universe_set = set(factor_df.index)

        # --- selected/optimized universe
        opt_universe = list(self.mother_universe_set.intersection(available_universe_set).intersection(factor_universe_set))
        opt_weight_df = factor_df.loc[opt_universe]

        if len(opt_weight_df) > 0:
            opt_weight_df = opt_weight_df.reset_index()
            opt_weight_df = opt_weight_df.sort_values(by=[self.factor, "instrument"], ascending=[False, True])
            opt_weight_df = self.cal_weight(opt_weight_df=opt_weight_df)

            # --- reformat
            opt_weight_df["contract"] = opt_weight_df["instrument"].map(lambda z: self.mgr_major.inquiry_major_contract(z, exe_date))
            opt_weight_df["price"] = opt_weight_df[["instrument", "contract"]].apply(
                lambda z: self.mgr_md.inquiry_price_at_date(z["contract"], z["instrument"], exe_date), axis=1)
            opt_weight_df["direction"] = opt_weight_df["opt"].map(lambda z: int(np.sign(z)))
            opt_weight_df["weight"] = opt_weight_df["opt"].abs()
            opt_weight_df = opt_weight_df.loc[opt_weight_df["weight"] > 0]

            if (len(opt_weight_df) < 2) and self.m_print_details:
                print("Warning! Not enough instruments in universe at sig_date = {}, exe_date = {}".format(sig_date, exe_date))
                print(available_universe_df)
                print(factor_df)

            return opt_weight_df[["contract", "price", "direction", "weight"]]
        else:
            return pd.DataFrame(data=None, columns=["contract", "price", "direction", "weight"])


# ------------------------------------------ Classes about trades -------------------------------------------------------------------
# --- custom type definition
TypeContract = NewType("TypeContract", str)
TypeDirection = NewType("TypeDirection", int)
TypePositionKey = NewType("TypeKey", tuple[TypeContract, TypeDirection])
TypeOperation = NewType("TypeOperation", int)

# --- custom CONST
CONST_DIRECTION_LONG: TypeDirection = TypeDirection(1)
CONST_DIRECTION_SHORT: TypeDirection = TypeDirection(-1)
CONST_OPERATION_OPEN: TypeOperation = TypeOperation(1)
CONST_OPERATION_CLOSE: TypeOperation = TypeOperation(-1)


# --- Class: Trade
class CTrade(object):
    def __init__(self, t_contract: TypeContract | str, t_direction: TypeDirection, t_operation: TypeOperation, t_quantity: int, t_instrument_id: str,
                 t_contract_multiplier: int):
        """

        :param t_contract: basically, trades are calculated from positions, so all the information can pe provided by positions, with only one exception : executed price
        :param t_direction:
        :param t_operation:
        :param t_quantity:
        :param t_instrument_id:
        :param t_contract_multiplier:
        """
        self.m_contract: TypeContract = t_contract
        self.m_direction: TypeDirection = t_direction
        self.m_key: TypePositionKey = TypePositionKey((t_contract, t_direction))

        self.m_instrument_id: str = t_instrument_id
        self.m_contract_multiplier: int = t_contract_multiplier

        self.m_operation: TypeOperation = t_operation
        self.m_quantity: int = t_quantity
        self.m_executed_price: float = 0

    def get_key(self) -> TypePositionKey:
        return self.m_key

    def get_tuple_trade_id(self) -> tuple[str, str]:
        return self.m_contract, self.m_instrument_id

    def get_tuple_execution(self) -> tuple[TypeOperation, int, float]:
        return self.m_operation, self.m_quantity, self.m_executed_price

    def operation_is(self, t_operation: TypeOperation) -> bool:
        return self.m_operation == t_operation

    def set_executed_price(self, t_executed_price: float):
        self.m_executed_price = t_executed_price


# --- Class: Position
class CPosition(object):
    def __init__(self, t_contract: TypeContract, t_direction: TypeDirection, t_instru_info: CInstrumentInfoTable):
        self.m_contract: TypeContract = t_contract
        self.m_direction: TypeDirection = t_direction
        self.m_key: TypePositionKey = TypePositionKey((t_contract, t_direction))
        self.m_instrument_id: str = parse_instrument_from_contract_wind(self.m_contract)
        self.m_contract_multiplier: int = t_instru_info.get_multiplier(t_instrument_id=self.m_instrument_id)
        self.m_quantity: int = 0

    def cal_quantity(self, t_price: float, t_allocated_mkt_val: float) -> 0:
        self.m_quantity = int(np.round(t_allocated_mkt_val / t_price / self.m_contract_multiplier))
        return 0

    def get_key(self) -> TypePositionKey:
        return self.m_key

    def get_tuple_pos_id(self) -> tuple[str, str]:
        return self.m_contract, self.m_instrument_id

    def get_quantity(self):
        return self.m_quantity

    def get_contract_multiplier(self) -> int:
        return self.m_contract_multiplier

    def is_empty(self) -> bool:
        return self.m_quantity == 0

    def cal_trade_from_other_pos(self, t_target: "CPosition") -> CTrade | None:
        """

        :param t_target: another position unit, usually new(target) position, must have the same key as self
        :return: None or a new trade
        """
        new_trade: CTrade | None = None
        delta_quantity: int = t_target.m_quantity - self.m_quantity
        if delta_quantity > 0:
            new_trade: CTrade = CTrade(
                t_contract=self.m_contract, t_direction=self.m_direction, t_operation=CONST_OPERATION_OPEN, t_quantity=delta_quantity,
                t_instrument_id=self.m_instrument_id, t_contract_multiplier=self.m_contract_multiplier
            )
        elif delta_quantity < 0:
            new_trade: CTrade = CTrade(
                t_contract=self.m_contract, t_direction=self.m_direction, t_operation=CONST_OPERATION_CLOSE, t_quantity=-delta_quantity,
                t_instrument_id=self.m_instrument_id, t_contract_multiplier=self.m_contract_multiplier
            )
        return new_trade

    def open(self):
        # Open new position
        new_trade: CTrade = CTrade(
            t_contract=self.m_contract, t_direction=self.m_direction, t_operation=CONST_OPERATION_OPEN, t_quantity=self.m_quantity,
            t_instrument_id=self.m_instrument_id, t_contract_multiplier=self.m_contract_multiplier
        )
        return new_trade

    def close(self):
        # Close old position
        new_trade: CTrade = CTrade(
            t_contract=self.m_contract, t_direction=self.m_direction, t_operation=CONST_OPERATION_CLOSE, t_quantity=self.m_quantity,
            t_instrument_id=self.m_instrument_id, t_contract_multiplier=self.m_contract_multiplier
        )
        return new_trade

    def to_dict(self) -> dict:
        return {
            "contact": self.m_contract,
            "direction": self.m_direction,
            "quantity": self.m_quantity,
            "contract_multiplier": self.m_contract_multiplier,
        }


# --- Class: PositionPlus
class CPositionPlus(CPosition):
    def __init__(self, t_contract: TypeContract, t_direction: TypeDirection, t_instru_info: CInstrumentInfoTable, t_cost_rate: float):
        super().__init__(t_contract, t_direction, t_instru_info)

        self.m_cost_price: float = 0
        self.m_last_price: float = 0
        self.m_unrealized_pnl: float = 0
        self.m_cost_rate: float = t_cost_rate

    def update_from_trade(self, t_trade: CTrade) -> dict:
        operation, quantity, executed_price = t_trade.get_tuple_execution()
        cost = executed_price * quantity * self.m_contract_multiplier * self.m_cost_rate

        realized_pnl = 0
        if operation == CONST_OPERATION_OPEN:
            amt_new = self.m_cost_price * self.m_quantity + executed_price * quantity
            self.m_quantity += quantity
            self.m_cost_price = amt_new / self.m_quantity
        if operation == CONST_OPERATION_CLOSE:
            realized_pnl = (executed_price - self.m_cost_price) * self.m_direction * self.m_contract_multiplier * quantity
            self.m_quantity -= quantity

        return {
            "contract": self.m_contract,
            "direction": self.m_direction,
            "operation": operation,
            "quantity": quantity,
            "price": executed_price,
            "cost": cost,
            "realized_pnl": realized_pnl,
        }

    def update_from_market_data(self, t_price: float) -> float:
        self.m_last_price = t_price
        self.m_unrealized_pnl = (self.m_last_price - self.m_cost_price) * self.m_direction * self.m_contract_multiplier * self.m_quantity
        return self.m_unrealized_pnl

    def update_from_last(self) -> float:
        self.m_unrealized_pnl = (self.m_last_price - self.m_cost_price) * self.m_direction * self.m_contract_multiplier * self.m_quantity
        return self.m_unrealized_pnl

    def to_dict(self) -> dict:
        d = super().to_dict()
        d.update(
            {
                "cost_price": self.m_cost_price,
                "last_price": self.m_last_price,
                "unrealized_pnl": self.m_unrealized_pnl,
            }
        )
        return d


# --- Class: Portfolio
class CPortfolio(object):
    def __init__(self, t_pid: str, t_init_cash: float, t_cost_reservation: float, t_cost_rate: float,
                 t_dir_pid: str, t_dir_pid_trades: str, t_dir_pid_positions: str,
                 t_save_details: bool = True):
        """

        :param t_pid: portfolio id
        :param t_init_cash:
        :param t_cost_reservation: absolute value, NOT multiplied by any RETURN SCALE
        :param t_cost_rate:absolute value, NOT multiplied by any RETURN SCALE
        """
        # basic
        self.m_pid: str = t_pid

        # pnl
        self.m_init_cash: float = t_init_cash
        self.m_realized_pnl_daily_details: list[dict] = []
        self.m_realized_pnl_daily_df: pd.DataFrame | None = None
        self.m_realized_pnl_daily: float = 0
        self.m_realized_pnl_cum: float = 0
        self.m_unrealized_pnl: float = 0
        self.m_nav: float = self.m_init_cash + self.m_realized_pnl_cum + self.m_unrealized_pnl

        # position
        self.m_manager_pos: dict[TypePositionKey, CPositionPlus] = {}

        # additional
        self.m_cost_reservation: float = t_cost_reservation
        self.m_cost_rate: float = t_cost_rate
        self.m_update_date: str = "YYYYMMDD"

        # save nav
        self.m_nav_daily_snapshots = []
        self.m_save_details: bool = t_save_details
        self.m_dir_pid: str = t_dir_pid
        self.m_dir_pid_trades: str = t_dir_pid_trades
        self.m_dir_pid_positions: str = t_dir_pid_positions

    def cal_target_position(self, t_new_pos_df: pd.DataFrame, t_instru_info: CInstrumentInfoTable) -> dict[TypePositionKey, CPosition]:
        """

        :param t_new_pos_df : a DataFrame with columns = ["contract", "price", "direction", "weight"]
                                a.1 this "price" is used to estimate how much quantity should be allocated
                                    for the instrument, CLOSE-PRICE is most frequently used, but other types
                                    such as OPEN could do the job as well. if new position is to open with T
                                    day's price, this "price" should be from T-1, which is available.
                                a.2 direction: 1 for long, -1 for short.
                                a.3 weight: non-negative value, sum of weights should not be greater than 1, if
                                    leverage are not allowed
        :param t_instru_info: an instance of CInstrumentInfoTable
        :return:
        """
        mgr_new_pos: dict[TypePositionKey, CPosition] = {}
        tot_allocated_amt = self.m_nav / (1 + self.m_cost_reservation)
        for contract, direction, price, weight in zip(t_new_pos_df["contract"], t_new_pos_df["direction"], t_new_pos_df["price"], t_new_pos_df["weight"]):
            tgt_pos = CPosition(t_contract=contract, t_direction=direction, t_instru_info=t_instru_info)
            tgt_pos.cal_quantity(t_price=price, t_allocated_mkt_val=tot_allocated_amt * weight)
            key, qty = tgt_pos.get_key(), tgt_pos.get_quantity()
            if qty > 0:
                mgr_new_pos[key] = tgt_pos
        return mgr_new_pos

    def cal_trades_for_signal(self, t_mgr_new_pos: dict[TypePositionKey, CPosition]) -> list[CTrade]:
        trades_list: list[CTrade] = []
        # cross comparison: step 0, check if new position is in old position
        for new_key, new_pos in t_mgr_new_pos.items():
            if new_key not in self.m_manager_pos:
                new_trade: CTrade = new_pos.open()
            else:
                new_trade: CTrade = self.m_manager_pos[new_key].cal_trade_from_other_pos(t_target=new_pos)  # could be none
            if new_trade is not None:
                trades_list.append(new_trade)

        # cross comparison: step 1, check if old position is in new position
        for old_key, old_pos in self.m_manager_pos.items():
            if old_key not in t_mgr_new_pos:
                new_trade: CTrade = old_pos.close()
                trades_list.append(new_trade)
        return trades_list

    def cal_trades_for_major(self, t_mgr_md: CManagerMarketData) -> list[CTrade]:
        trades_list: list[CTrade] = []
        for old_key, old_pos in self.m_manager_pos.items():
            old_contract, instrument_id = old_pos.get_tuple_pos_id()
            new_contract = t_mgr_md.inquiry_major_contract(instrument_id=instrument_id, trade_date=self.m_update_date)
            if old_contract != new_contract:
                trade_close_old = old_pos.close()
                trade_open_new = CTrade(
                    t_contract=new_contract, t_direction=old_pos.get_key()[1],
                    t_operation=CONST_OPERATION_OPEN, t_quantity=old_pos.get_quantity(),
                    t_instrument_id=instrument_id, t_contract_multiplier=old_pos.get_contract_multiplier()
                )
                trades_list.append(trade_close_old)
                trades_list.append(trade_open_new)
        return trades_list

    def update_from_trades(self, t_trades_list: list[CTrade], t_instru_info: CInstrumentInfoTable):
        # trades loop
        for trade in t_trades_list:
            trade_key = trade.get_key()
            if trade_key not in self.m_manager_pos:
                self.m_manager_pos[trade_key] = CPositionPlus(
                    t_contract=trade_key[0], t_direction=trade_key[1],
                    t_instru_info=t_instru_info, t_cost_rate=self.m_cost_rate
                )
            trade_result = self.m_manager_pos[trade_key].update_from_trade(t_trade=trade)
            self.m_realized_pnl_daily_details.append(trade_result)

        # remove empty trade
        for pos_key in list(self.m_manager_pos.keys()):
            if self.m_manager_pos[pos_key].is_empty():
                del self.m_manager_pos[pos_key]
        return 0

    def initialize_daily(self, t_trade_date) -> int:
        self.m_update_date = t_trade_date
        self.m_realized_pnl_daily_details = []
        self.m_realized_pnl_daily_df = None
        self.m_realized_pnl_daily = 0
        return 0

    def update_unrealized_pnl(self, t_mgr_md: CManagerMarketData) -> int:
        self.m_unrealized_pnl = 0
        for pos in self.m_manager_pos.values():
            contract, instrument_id = pos.get_tuple_pos_id()
            last_price = t_mgr_md.inquiry_price_at_date(
                contact=contract, instrument=instrument_id, trade_date=self.m_update_date,
                price_type="close"
            )  # always use close to estimate the unrealized pnl
            if np.isnan(last_price):
                print("nan price for {} {}".format(contract, self.m_update_date))
                self.m_unrealized_pnl += pos.update_from_last()
            else:
                self.m_unrealized_pnl += pos.update_from_market_data(t_price=last_price)
        return 0

    def update_realized_pnl(self) -> int:
        if len(self.m_realized_pnl_daily_details) > 0:
            self.m_realized_pnl_daily_df = pd.DataFrame(self.m_realized_pnl_daily_details)
            self.m_realized_pnl_daily = self.m_realized_pnl_daily_df["realized_pnl"].sum() - self.m_realized_pnl_daily_df["cost"].sum()
        self.m_realized_pnl_cum += self.m_realized_pnl_daily
        return 0

    def update_nav(self) -> int:
        self.m_nav = self.m_init_cash + self.m_realized_pnl_cum + self.m_unrealized_pnl
        return 0

    def save_nav_snapshots(self) -> int:
        d = {
            "trade_date": self.m_update_date,
            "realized_pnl_daily": self.m_realized_pnl_daily,
            "realized_pnl_cum": self.m_realized_pnl_cum,
            "unrealized_pnl": self.m_unrealized_pnl,
            "nav": self.m_nav,
            "navps": self.m_nav / self.m_init_cash
        }
        self.m_nav_daily_snapshots.append(d)
        return 0

    def save_position(self) -> int:
        # format to DataFrame
        pos_data_list = []
        for pos in self.m_manager_pos.values():
            pos_data_list.append(pos.to_dict())
        pos_df = pd.DataFrame(pos_data_list)

        # save to csv
        if self.m_save_details:
            pos_file = "{}.{}.positions.csv.gz".format(self.m_pid, self.m_update_date)
            pos_path = os.path.join(self.m_dir_pid_positions, pos_file)
            pos_df.to_csv(pos_path, index=False, float_format="%.6f", compression="gzip")
        return 0

    def save_trades(self) -> int:
        if self.m_save_details and (self.m_realized_pnl_daily_df is not None):
            records_trades_file = "{}.{}.trades.csv.gz".format(self.m_pid, self.m_update_date)
            records_trades_path = os.path.join(self.m_dir_pid_trades, records_trades_file)
            self.m_realized_pnl_daily_df.to_csv(records_trades_path, index=False, float_format="%.6f", compression="gzip")
        return 0

    def save_nav(self) -> int:
        nav_daily_df = pd.DataFrame(self.m_nav_daily_snapshots)
        nav_daily_file = "{}.nav.daily.csv.gz".format(self.m_pid)
        nav_daily_path = os.path.join(self.m_dir_pid, nav_daily_file)
        nav_daily_df.to_csv(nav_daily_path, index=False, float_format="%.4f", compression="gzip")
        return 0

    def main_loop(self, t_simu_bgn_date: str, t_simu_stp_date: str, t_start_delay: int, t_hold_period_n: int,
                  t_trade_calendar: CCalendar, t_instru_info: CInstrumentInfoTable,
                  t_mgr_signal: CManagerSignalBase, t_mgr_md: CManagerMarketData):
        iter_trade_dates_list = t_trade_calendar.get_iter_list(t_bgn_date=t_simu_bgn_date, t_stp_date=t_simu_stp_date, t_ascending=True)
        for ti, trade_date in enumerate(iter_trade_dates_list):
            # --- initialize
            signal_date = t_trade_calendar.get_next_date(t_this_date=trade_date, t_shift=-1)
            self.initialize_daily(t_trade_date=trade_date)

            # --- check signal and cal new positions
            if (ti - t_start_delay) % t_hold_period_n == 0:  # ti is a execution date
                new_pos_df = t_mgr_signal.cal_new_pos(sig_date=signal_date, exe_date=trade_date)
                mgr_new_pos = self.cal_target_position(t_new_pos_df=new_pos_df, t_instru_info=t_instru_info)  # Type(mgr_new_pos)=dict[TypeKey, CPosition]
                array_new_trades = self.cal_trades_for_signal(t_mgr_new_pos=mgr_new_pos)
                # no major-shift check is necessary
                # because signal would contain this information itself already
            else:
                # array_new_trades = []
                array_new_trades = self.cal_trades_for_major(t_mgr_md=t_mgr_md)  # use this function to check for major-shift

            for new_trade in array_new_trades:
                contract, instrument_id = new_trade.get_tuple_trade_id()
                executed_price = t_mgr_md.inquiry_price_at_date(contact=contract, instrument=instrument_id, trade_date=trade_date, price_type="close")
                new_trade.set_executed_price(t_executed_price=executed_price)
            self.update_from_trades(t_trades_list=array_new_trades, t_instru_info=t_instru_info)

            # --- update with market data for realized and unrealized pnl
            self.update_realized_pnl()
            self.update_unrealized_pnl(t_mgr_md=t_mgr_md)
            self.update_nav()

            # --- save snapshots
            self.save_trades()
            self.save_position()
            self.save_nav_snapshots()

        # save nav
        self.save_nav()
        return 0
