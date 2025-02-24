import numpy as np
import pandas as pd
import datetime as dt
from rich.live import Live
from rich.table import Table
from rich.box import HORIZONTALS
from tqsdk import TqApi, TqAuth
from dataclasses import dataclass, astuple
from husfort.qinstruments import CInstruMgr, parse_instrument_from_contract


@dataclass(frozen=True)
class CAccountTianqin:
    userId: str
    password: str


@dataclass(frozen=True)
class CCfgGeneral:
    NewScreen: int = 1


@dataclass(frozen=True)
class CCfgColor:
    # 一般列字体颜色
    OtherFontPos: str = "#DA0B0B"
    OtherFontNeg: str = "#008E00"
    OtherFontZero: str = "#A0A0A0"

    # 一般列背景颜色
    OtherBackgroundPos: str = None
    OtherBackgroundNeg: str = None
    OtherBackgroundZero: str = None

    # 收益列字体颜色
    PnlFontPos: str = "#FFFFFF"
    PnlFontNeg: str = "#FFFFFF"
    PnlFontZero: str = "#FFFFFF"

    # 收益列背景颜色
    PnlBackgroundPos: str = "#8B0000"
    PnlBackgroundNeg: str = "#006400"
    PnlBackgroundZero: str = "#606060"

    # 上标题字体颜色
    TitleFont: str = "#0087AF"

    # 下标题字体颜色
    CaptionFont: str = "#0087AF"

    # 列名行字体颜色
    HeaderFont: str = "#FFFFFF"

    # 列名行背景颜色
    HeaderBackground: str = "#D75F00"

    # 合计行字体颜色
    FooterFont: str = "#FFFFFF"


@dataclass(frozen=True)
class CCfg:
    account: CAccountTianqin
    general: CCfgGeneral = CCfgGeneral()
    color: CCfgColor = CCfgColor()


@dataclass
class CContract:
    contractId: str
    instrumentId: str
    exchangeId: str
    multiplier: int

    @staticmethod
    def gen(contractId: str, instru_mgr: CInstruMgr) -> "CContract":
        instrument = parse_instrument_from_contract(contractId)
        exchange = instru_mgr.get_exchange(instrument)
        multiplier = instru_mgr.get_multiplier(instrument)
        return CContract(
            contractId=contractId,
            instrumentId=instrument,
            exchangeId=exchange,
            multiplier=multiplier,
        )

    @property
    def tianqin_id(self) -> str:
        return f"{self.exchangeId}.{self.contractId}"

    def __gt__(self, other: "CContract"):
        return self.contractId > other.contractId


@dataclass
class CPosition:
    contract: CContract
    direction: int
    qty: int
    base_price: float
    last_price: float = np.nan

    @property
    def base_val(self) -> float:
        return self.base_price * self.qty * self.contract.multiplier * self.direction

    @property
    def last_val(self) -> float:
        return self.last_price * self.qty * self.contract.multiplier * self.direction

    @property
    def float_pnl(self) -> float:
        return self.last_val - self.base_val

    def __eq__(self, other: "CPosition"):
        return self.float_pnl == other.float_pnl

    def __gt__(self, other: "CPosition"):
        if self.float_pnl > other.float_pnl:
            return True
        elif self.float_pnl < other.float_pnl:
            return False
        else:
            if self.contract > other.contract:
                return True
            elif self.contract < other.contract:
                return False
            else:
                return self.direction > other.direction

    def set_last_price(self, price: float):
        if not np.isnan(price):
            self.last_price = price


@dataclass
class CRow:
    contractId: str
    dir: str
    qty: str
    base: str
    last: str
    base_val: str
    last_val: str
    float_pnl: str


class CManagerViewer:
    def __init__(self, positions: list[CPosition], config: CCfg):
        self.positions: list[CPosition] = positions
        self.user_choice: str = ""
        self.pos_and_quotes_df = pd.DataFrame()
        self.config = config

    @property
    def positions_size(self) -> int:
        return len(self.positions)

    @property
    def new_screen(self) -> bool:
        return self.config.general.NewScreen == 1

    def set_color(self, x: float) -> tuple[str, str, str, str]:
        if x > 0:
            return (self.config.color.OtherFontPos,
                    self.config.color.OtherBackgroundPos,
                    self.config.color.PnlFontPos,
                    self.config.color.PnlBackgroundPos)
        elif x < 0:
            return (self.config.color.OtherFontNeg,
                    self.config.color.OtherBackgroundNeg,
                    self.config.color.PnlFontNeg,
                    self.config.color.PnlBackgroundNeg)
        else:
            return (self.config.color.OtherFontZero,
                    self.config.color.OtherBackgroundZero,
                    self.config.color.PnlFontZero,
                    self.config.color.PnlBackgroundZero)

    def __update_rows_and_footer(self) -> tuple[list[CRow], CRow]:
        qty = 0
        base_val, last_val, float_pnl = 0.0, 0.0, 0.0
        rows: list[CRow] = []
        for pos in self.pos_and_quotes_df["pos"]:
            pos: CPosition
            qty += pos.qty
            base_val += pos.base_val
            last_val += pos.last_val
            float_pnl += pos.float_pnl
            color_other_font, color_other_bg, color_pnl_font, color_pnl_bg = self.set_color(pos.float_pnl)
            sty_other, sty_pnl = f"[{color_other_font} on {color_other_bg}]", f"[{color_pnl_font} on {color_pnl_bg}]"
            rows.append(CRow(
                contractId=f"{sty_other}{pos.contract.contractId}",
                dir=f"{sty_other}{pos.direction}",
                qty=f"{sty_other}{pos.qty}",
                base=f"{sty_other}{pos.base_price:10.2f}",
                last=f"{sty_other}{pos.last_price:10.2f}",
                base_val=f"{sty_other}{pos.base_val:12.2f}",
                last_val=f"{sty_other}{pos.last_val:12.2f}",
                float_pnl=f"{sty_other}{pos.float_pnl:10.2f}",
            ))

        # set footer
        _, _, color_pnl_font, color_pnl_bg = self.set_color(float_pnl)
        sty_footer = f"[{color_pnl_font} on {color_pnl_bg}]"
        footer = CRow(
            contractId=f"{sty_footer}{'SUM'.rjust(8)}",
            dir=f"{sty_footer}{'-'.rjust(3)}",
            qty=f"{sty_footer}{qty:>3d}",
            base=f"{sty_footer}{'-'.rjust(10)}",
            last=f"{sty_footer}{'-'.rjust(10)}",
            base_val=f"{sty_footer}{base_val:12.2f}",
            last_val=f"{sty_footer}{last_val:12.2f}",
            float_pnl=f"{sty_footer}{float_pnl:10.2f}",
        )
        return rows, footer

    def __generate_table(self) -> Table:
        rows, footer = self.__update_rows_and_footer()
        table = Table(
            title=f"\nPNL INCREMENT - {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}",
            caption="Press Ctrl + C to quit ...",
            box=HORIZONTALS,
            title_style=f"bold {self.config.color.TitleFont}",
            caption_style=f"bold italic {self.config.color.CaptionFont}",
            header_style=f"bold {self.config.color.HeaderFont} on {self.config.color.HeaderBackground}",
            footer_style=f"{self.config.color.FooterFont}",
            show_footer=True,
            padding=0,
        )
        table.add_column(header="CONTRACT", justify="right", footer=footer.contractId)
        table.add_column(header="DIR", justify="right", footer=footer.dir)
        table.add_column(header="QTY", justify="right", footer=footer.qty)
        table.add_column(header="BASE", justify="right", footer=footer.base)
        table.add_column(header="LAST", justify="right", footer=footer.last)
        table.add_column(header="BASE-VAL", justify="right", footer=footer.base_val)
        table.add_column(header="LAST-VAL", justify="right", footer=footer.last_val)
        table.add_column(header="FLOAT-PNL", justify="right", footer=footer.float_pnl)
        for row in rows:
            table.add_row(*astuple(row))
        return table

    def create_quotes_df(self, tq_account: str, tq_password: str) -> TqApi:
        contracts = [pos.contract.tianqin_id for pos in self.positions]
        api = TqApi(auth=TqAuth(user_name=tq_account, password=tq_password))
        quotes = [api.get_quote(contract) for contract in contracts]
        self.pos_and_quotes_df = pd.DataFrame({"pos": self.positions, "quote": quotes})
        return api

    def update_from_quotes(self):
        for pos, quote in zip(self.pos_and_quotes_df["pos"], self.pos_and_quotes_df["quote"]):
            pos.last_price = quote.last_price
        self.pos_and_quotes_df.sort_values(by="pos", ascending=False, inplace=True)
        return 0

    def main(self):
        api = self.create_quotes_df(self.config.account.userId, self.config.account.password)
        try:
            with Live(self.__generate_table(), auto_refresh=False, screen=self.new_screen) as live:
                while True:
                    api.wait_update()
                    self.update_from_quotes()
                    live.update(self.__generate_table(), refresh=True)
        except KeyboardInterrupt:
            print("\n", end="")
            api.close()
