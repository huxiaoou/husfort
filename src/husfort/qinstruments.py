import re
import pandas as pd
from typing import Literal
from dataclasses import dataclass


def parse_instrument_from_contract(contract: str) -> str:
    return re.sub(pattern="[0-9]", repl="", string=contract)


@dataclass(frozen=True)
class CInstrument:
    instrumentId: str
    exchange: str
    minispread: float
    multiplier: int
    timeTableId: str
    hasNgtSec: int
    hasDayBrk: int
    ngtSec: str
    daySec0: str
    daySec1: str
    daySec2: str
    windId: str
    tushareId: str


class CInstruMgr(object):
    def __init__(self, instru_info_path: str, key: Literal["instrumentId", "windId", "tushareId"] = "instrumentId",
                 file_type: Literal["EXCEL", "CSV"] = "CSV", sheet_name: str = "instruments"):
        """

        :param instru_info_path: InstrumentInfo file path, could be a txt(csv) or xlsx
        :param key: "instrumentId"(like "a.dce") or "windCode"(like "A.DCE")
        :param file_type: "EXCEL" for xlsx, others for txt(csv)
        :param sheet_name: default = "instruments", only used if file_type = "EXCEL"
        """
        dtype_table = {
            "instrumentId": str,
            "exchange": str,
            "minispread": float,
            "multiplier": int,
            "timeTableId": str,
            "hasNgtSec": int,
            "hasDayBrk": int,
            "ngtSec": str,
            "daySec0": str,
            "daySec1": str,
            "daySec2": str,
            "windId": str,
            "tushareId": str,
        }

        if file_type.upper() == "EXCEL":
            self.instruments_data = pd.read_excel(instru_info_path, sheet_name=sheet_name, dtype=dtype_table)
        elif file_type.upper() == "CSV":
            self.instruments_data = pd.read_csv(instru_info_path, dtype=dtype_table)
        else:
            raise ValueError(f"Invalid file type = {file_type}.")

        self.mgr: dict[str, CInstrument] = {}
        for instru_data in self.instruments_data.to_dict(orient="index").values():
            self.mgr[instru_data[key]] = CInstrument(**instru_data)

    def get_universe(self, cformat: Literal["VANILLA", "WIND", "TUSHARE"] = "VANILLA") -> list[str]:
        """

        :param cformat: code format
        :return:
        """
        if cformat.upper() == "VANILLA":
            return [v.instrumentId for v in self.mgr.values()]
        elif cformat.upper() == "WIND":
            return [v.windId for v in self.mgr.values()]
        elif cformat.upper() == "TUSHARE":
            return [v.tushareId for v in self.mgr.values()]
        else:
            raise ValueError(f"Invalid cformat = {cformat}.")

    def get_multiplier(self, instrumentId: str) -> int:
        return self.mgr[instrumentId].multiplier

    def get_multiplier_from_contract(self, contract: str) -> int:
        instrumentId = parse_instrument_from_contract(contract)
        return self.get_multiplier(instrumentId)

    def get_mini_spread(self, instrumentId: str) -> float:
        return self.mgr[instrumentId].minispread

    def get_exchange(self, instrumentId: str, cformat: Literal["VANILLA", "WIND", "TUSHARE"] = "VANILLA") -> str:
        if cformat.upper() == "VANILLA":
            return self.mgr[instrumentId].exchange
        elif cformat.upper() == "WIND":
            return self.mgr[instrumentId].windId.split(".")[-1]
        elif cformat.upper() == "TUSHARE":
            return self.mgr[instrumentId].tushareId.split(".")[-1]
        else:
            raise ValueError(f"Invalid cformat = {cformat}.")

    def get_exchange_chs(self, instrument_id: str) -> str:
        exchange_id_full = self.get_exchange(instrument_id, cformat="VANILLA")
        exchange_id_chs = {
            "DCE": "大商所",
            "CZCE": "郑商所",
            "SHFE": "上期所",
            "INE": "上海能源",
            "GFE": "广期所",
            "CFFEX": "中金所",
        }[exchange_id_full]
        return exchange_id_chs

    def get_windId(self, instrument_id: str) -> str:
        return self.mgr[instrument_id].windId

    def get_ngt_sec_end_hour(self, instrument_id: str):
        return self.instruments_data.at[instrument_id, "ngtSecEndHour"]

    def get_ngt_sec_end_minute(self, instrument_id: str):
        return self.instruments_data.at[instrument_id, "ngtSecEndMinute"]

    def convert_contract_from_vanilla(self, contract: str, cformat: Literal["WIND", "TUSHARE"]) -> str:
        """
    
        :param contract: general contract id, such as "j2209"
        :param cformat: "WIND", "TUSHARE"
        :return: "J2209.DCE"
        """

        _instrumentId = parse_instrument_from_contract(contract=contract)
        _exchange = self.get_exchange(instrumentId=_instrumentId, cformat=cformat)
        return contract.upper() + "." + _exchange

    @staticmethod
    def convert_contract_to_vanilla(contract_id: str, cformat: Literal["WIND", "TUSHARE"]) -> str:
        """

        :param contract_id: general contract id, such as "CF2209.ZCE" or "CF2209.CZC"
        :param cformat: "WIND", "TUSHARE"
        :return: "CF2209"
        """
        cid, exchange = contract_id.split(".")
        if cformat.upper() == "WIND":
            return cid if exchange in ["CFE", "CZC"] else cid.lower()
        elif cformat.upper() == "TUSHARE":
            return cid if exchange in ["CFX", "ZCE"] else cid.lower()
        else:
            raise ValueError(f"Invalid cformat = {cformat}.")

    def fix_contract_id(self, contract: str, trade_date: str, cformat: Literal["VANILLA", "WIND", "TUSHARE"]) -> str:
        """

        :param contract: it should have a format like "MA105" or "MA105.CZC", in which "05" = May
                  however "1" is ambiguous, since it could be either "2011" or "2021"
                  this function is designed to solve this problem
        :param trade_date: on which day, this contract is traded
        :param cformat: "wind" or "vanilla"
        :return:
        """

        if cformat.upper() == "VANILLA":  # "MA105"
            instrumentId = parse_instrument_from_contract(contract)  # "MA"
            exchange = self.get_exchange(instrumentId)  # "CZC"
            len_cid, len_instru_id = len(contract), len(instrumentId)  # len("MA105"), len("MA")
        elif cformat.upper() in ["WIND", "TUSHARE"]:  # "MA105.CZC"
            cid, exchange = contract.split(".")  # "MA105", "CZC"
            instrumentId = parse_instrument_from_contract(cid)  # "MA"
            len_cid, len_instru_id = len(cid), len(instrumentId)  # len("MA105"), len("MA")
        else:
            raise ValueError(f"Invalid cformat = {cformat}.")

        if exchange != "CZCE":
            # this problem only happens for CZCE
            return contract

        if re.match(pattern=r"^[a-zA-Z]{1,2}[\d]{4}$", string=contract) is not None:
            # some old contract did have format like "MA1105"
            # in this case Nothing should be done
            return contract

        td = int(trade_date[2])  # decimal year to be inserted, "X" in "20XYMMDD"
        ty = int(trade_date[3])  # trade year number,           "Y" in "20XYMMDD"
        cy = int(contract[len_instru_id])  # contract year, "1" in "MA105" format = "**YMM"
        if cy < ty:
            # contract year should always >= the trade year
            # if not, decimal year +=1
            td += 1
        return contract[0:len_instru_id] + str(td) + contract[len_instru_id:]
