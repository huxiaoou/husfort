import os
import datetime as dt
import pandas as pd
from dataclasses import dataclass, field


class CCalendar(object):
    def __init__(self, calendar_path: os.path, header: int = 0):
        if isinstance(header, int):
            calendar_df = pd.read_csv(calendar_path, dtype=str, header=header)
        else:
            calendar_df = pd.read_csv(calendar_path, dtype=str, header=None, names=["trade_date"])
        self.__trade_dates = [_.replace("-", "") for _ in calendar_df["trade_date"]]

    @property
    def last_date(self):
        return self.__trade_dates[-1]

    @property
    def first_date(self):
        return self.__trade_dates[0]

    @property
    def trade_dates(self) -> list[str]:
        return self.__trade_dates

    def get_iter_list(self, bgn_date: str, stp_date: str, ascending: bool = True):
        res = []
        for t_date in self.__trade_dates:
            if t_date < bgn_date:
                continue
            if t_date >= stp_date:
                break
            res.append(t_date)
        return res if ascending else sorted(res, reverse=True)

    def get_sn(self, base_date: str):
        return self.__trade_dates.index(base_date)

    def get_date(self, sn: int):
        return self.__trade_dates[sn]

    def get_next_date(self, this_date: str, shift: int = 1):
        """

        :param this_date:
        :param shift: > 0, get date in the future; < 0, get date in the past
        :return:
        """

        this_sn = self.get_sn(this_date)
        next_sn = this_sn + shift
        return self.__trade_dates[next_sn]

    @staticmethod
    def move_date_string(trade_date: str, move_days: int = 1) -> str:
        """

        :param trade_date:
        :param move_days: >0, to the future; <0, to the past
        :return:
        """
        return (dt.datetime.strptime(trade_date, "%Y%m%d") + dt.timedelta(days=move_days)).strftime("%Y%m%d")


CONST_TS1, CONST_TS2 = "TS1", "TS2"


@dataclass(frozen=True)
class CSection(object):
    trade_date: str
    section: str
    bgnTime: str
    endTime: str
    secId: str = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "secId", f"{self.trade_date}-{self.section}")

    def __le__(self, other: "CSection"):
        return self.secId <= other.secId

    def __lt__(self, other: "CSection"):
        return self.secId < other.secId

    def match(self, tp: str):
        return self.bgnTime <= tp <= self.endTime


class CCalendarSection(object):
    def __init__(
            self,
            calendar_path: str,
            header: int = None,
            ts1_bgn_time: str = "19:00:00.000000",
            ts1_end_time: str = "07:00:00.000000",
            ts2_bgn_time: str = "07:00:00.000000",
            ts2_end_time: str = "19:00:00.000000",
    ):
        self.sections: list[CSection] = []
        if header:
            df = pd.read_csv(calendar_path, dtype=str, header=header)
        else:
            df = pd.read_csv(calendar_path, dtype=str, header=None, names=["trade_date"])
        for this_trade_date, next_trade_date in zip(df["trade_date"][:-1], df["trade_date"][1:]):
            next_date = (dt.datetime.strptime(this_trade_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

            # this section TS2
            bgn_time = f"{this_trade_date} {ts2_bgn_time}"
            end_time = f"{this_trade_date} {ts2_end_time}"
            this_sec_ts2 = CSection(trade_date=this_trade_date, section=CONST_TS2, bgnTime=bgn_time, endTime=end_time)
            self.sections.append(this_sec_ts2)

            # next section TS1
            bgn_time = f"{this_trade_date} {ts1_bgn_time}"
            end_time = f"{next_date} {ts1_end_time}"
            next_sec_ts1 = CSection(trade_date=next_trade_date, section=CONST_TS1, bgnTime=bgn_time, endTime=end_time)
            self.sections.append(next_sec_ts1)

        self.sections_size = len(self.sections)

    def head(self, n: int) -> list[CSection]:
        return self.sections[0:n]

    def tail(self, n: int) -> list[CSection]:
        return self.sections[-n:]

    def get_sn(self, sec: CSection) -> int:
        return self.sections.index(sec)

    def get_next_sec(self, this_sec: CSection, shift: int = 1) -> CSection | None:
        sn = self.get_sn(this_sec)
        sn_dst = sn + shift
        if 0 <= sn_dst < self.sections_size:
            return self.sections[sn_dst]
        else:
            return None

    def get_iter_list(self, bgn_sec: CSection, stp_sec: CSection) -> list[CSection]:
        iter_list: list[CSection] = []
        for sec in self.sections:
            if sec >= stp_sec:
                break
            if sec >= bgn_sec:
                iter_list.append(sec)
        return iter_list

    def match(self, tp: str) -> tuple[bool, CSection | None]:
        for sec in self.sections:
            if sec.match(tp):
                return True, sec
        return False, None

    def match_id(self, tgt_sec_id: str) -> tuple[bool, CSection | None]:
        for sec in self.sections:
            if sec.secId == tgt_sec_id:
                return True, sec
        return False, None

    def match_date(self, tgt_date: str) -> tuple[bool, list[CSection]]:
        res = [sec for sec in self.sections if sec.trade_date == tgt_date]
        return (True, res) if res else (False, res)

    def parse_section(self, using_now: bool, bgn_sec_id: str, stp_sec_id: str) -> tuple[bool, tuple[CSection, CSection]]:
        if using_now:
            tp = dt.datetime.now().strftime("%Y%m%d %H:%M:%S.%f")
            _, b = self.match(tp)
            s = self.get_next_sec(b) if _ else None
        else:
            _, b = self.match_id(bgn_sec_id)
            _, s = self.match_id(stp_sec_id)
            if b is not None:
                if s is None:
                    s = self.get_next_sec(b, 1)
            else:
                s = None
        parse_success = (b is not None) and (s is not None)
        return parse_success, (b, s)
