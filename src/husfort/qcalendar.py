import datetime as dt
import pandas as pd
from dataclasses import dataclass, field


class CCalendar(object):
    def __init__(self, calendar_path: str, header: int = 0):
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

    def get_iter_list(self, bgn_date: str, stp_date: str, ascending: bool = True) -> list[str]:
        res = []
        for t_date in self.__trade_dates:
            if t_date < bgn_date:
                continue
            if t_date >= stp_date:
                break
            res.append(t_date)
        return res if ascending else sorted(res, reverse=True)

    def shift_iter_dates(self, iter_dates: list[str], shift: int) -> list[str]:
        """

        :param iter_dates:
        :param shift: > 0, in the future
                      < 0, in the past
        :return:
        """
        if shift >= 0:
            new_dates = [self.get_next_date(iter_dates[-1], shift=s) for s in range(1, shift + 1)]
            shift_dates = iter_dates[shift:] + new_dates
        else:  # shift < 0
            new_dates = [self.get_next_date(iter_dates[0], shift=s) for s in range(shift, 0)]
            shift_dates = new_dates + iter_dates[:shift]
        return shift_dates

    def get_sn(self, base_date: str) -> int:
        return self.__trade_dates.index(base_date)

    def get_date(self, sn: int) -> str:
        return self.__trade_dates[sn]

    def has_date(self, trade_date: str) -> bool:
        return trade_date in self.__trade_dates

    def get_next_date(self, this_date: str, shift: int = 1) -> str:
        """

        :param this_date:
        :param shift: > 0, get date in the future; < 0, get date in the past
        :return:
        """

        this_sn = self.get_sn(this_date)
        next_sn = this_sn + shift
        return self.__trade_dates[next_sn]

    def get_start_date(self, bgn_date: str, max_win: int, shift: int) -> str:
        return self.get_next_date(bgn_date, -max_win + shift)

    def get_last_days_in_range(self, bgn_date: str, stp_date: str) -> list[str]:
        res = []
        for this_day, next_day in zip(self.__trade_dates[:-1], self.__trade_dates[1:]):
            if this_day < bgn_date:
                continue
            elif this_day >= stp_date:
                break
            else:
                if this_day[0:6] != next_day[0:6]:
                    res.append(this_day)
        return res

    def get_last_day_of_month(self, month: str) -> str:
        """
        :param month: like "202403"

        """

        threshold = f"{month}31"
        for t in self.__trade_dates[::-1]:
            if t <= threshold:
                return t
        raise ValueError(f"Could not find last day for {month}")

    def get_first_day_of_month(self, month: str) -> str:
        """
        :param month: like 202403

        """

        threshold = f"{month}01"
        for t in self.__trade_dates:
            if t >= threshold:
                return t
        raise ValueError(f"Could not find first day for {month}")

    @staticmethod
    def split_by_month(dates: list[str]) -> dict[str, list[str]]:
        res = {}
        for t in dates:
            m = t[0:6]
            if m not in res:
                res[m] = [t]
            else:
                res[m].append(t)
        return res

    def get_week_end_days_in_range(self, bgn_date: str, stp_date: str) -> list[str]:
        res = []
        for this_day, next_day in zip(self.__trade_dates[:-1], self.__trade_dates[1:]):
            if this_day < bgn_date:
                continue
            elif this_day >= stp_date:
                break
            else:
                d0 = dt.datetime.strptime(this_day, "%Y%m%d")
                d1 = dt.datetime.strptime(next_day, "%Y%m%d")
                if (d1 - d0).days > 1:
                    res.append(this_day)
        return res

    @staticmethod
    def split_by_week_end_days(
            dates: list[str], week_end_days: list[str], ascending: bool = True,
    ) -> dict[str, list[str]]:
        """

        :param dates:
        :param week_end_days: must come from dates, in other words, the following two condition must be true:
                              1.    dates = self.get_iter_list(bgn_date, stp_date)
                              2.    week_end_days = self.get_week_end_days_in_range(bgn_date, stp_date).
                              Or unpredicted bugs may happen.
        :param ascending: if true, the result will be sorted by week_end_days in ascending order.
        :return:
        """
        res: dict[str, list[str]] = {}
        week_end_day_idx = len(week_end_days) - 1
        if week_end_day_idx < 0:
            return res
        week_end_day = week_end_days[week_end_day_idx]
        buffer_dates = []
        for trade_date in dates[::-1]:
            if trade_date <= week_end_day:
                res[week_end_day] = buffer_dates
                week_end_day_idx -= 1
                if week_end_day_idx < 0:
                    break
                week_end_day = week_end_days[week_end_day_idx]
                buffer_dates = []
            buffer_dates.insert(0, trade_date)
        if ascending:
            return {k: res[k] for k in sorted(res)}
        else:
            return res

    @staticmethod
    def move_date_string(trade_date: str, move_days: int = 1) -> str:
        """

        :param trade_date:
        :param move_days: >0, to the future; <0, to the past
        :return:
        """
        return (dt.datetime.strptime(trade_date, "%Y%m%d") + dt.timedelta(days=move_days)).strftime("%Y%m%d")

    @staticmethod
    def convert_d08_to_d10(date: str) -> str:
        # "202100101" -> "2021-01-01"
        return date[0:4] + "-" + date[4:6] + "-" + date[6:8]

    @staticmethod
    def convert_d10_to_d08(date: str) -> str:
        # "20210-01-01" -> "20210101"
        return date.replace("-", "")

    @staticmethod
    def get_next_month(month: str, s: int) -> str:
        """

        :param month: format = YYYYMM
        :param s: > 0 in the future
                  < 0 in the past
        :return:
        """
        y, m = int(month[0:4]), int(month[4:6])
        dy, dm = s // 12, s % 12
        ny, nm = y + dy, m + dm
        if nm > 12:
            ny, nm = ny + 1, nm - 12
        return f"{ny:04d}{nm:02d}"

    def get_dates_header(self, bgn_date: str, stp_date: str, header_name: str = "trade_date") -> pd.DataFrame:
        """
        :param bgn_date: format = "YYYYMMDD"
        :param stp_date: format = "YYYYMMDD"
        :param header_name:
        :return:
        """

        h = pd.DataFrame({header_name: self.get_iter_list(bgn_date, stp_date)})
        return h


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

    def __ge__(self, other: "CSection"):
        return self.secId >= other.secId

    def __gt__(self, other: "CSection"):
        return self.secId > other.secId

    def __eq__(self, other: "CSection"):
        return self.secId == other.secId

    def match(self, tp: str) -> bool:
        return self.bgnTime <= tp <= self.endTime


class CCalendarSection(object):
    def __init__(
            self,
            calendar_path: str,
            header: int | None = None,
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

    def parse_section(
            self, using_now: bool, bgn_sec_id: str, stp_sec_id: str
    ) -> tuple[bool, tuple[CSection | None, CSection | None]]:
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


@dataclass(frozen=True)
class CMonth(object):
    trade_month: str
    trade_dates: tuple[str]

    def __le__(self, other: "CMonth"):
        return self.trade_month <= other.trade_month

    def __lt__(self, other: "CMonth"):
        return self.trade_month < other.trade_month

    def __ge__(self, other: "CMonth"):
        return self.trade_month >= other.trade_month

    def __gt__(self, other: "CMonth"):
        return self.trade_month > other.trade_month

    def __eq__(self, other: "CMonth"):
        return self.trade_month == other.trade_month

    @property
    def bgn_date(self) -> str:
        return self.trade_dates[0]

    @property
    def end_date(self) -> str:
        return self.trade_dates[-1]

    def match_from_date(self, trade_date: str) -> bool:
        return self.bgn_date <= trade_date <= self.end_date

    def match_from_month(self, month_id: str) -> bool:
        return self.trade_month == month_id


class CCalendarMonth(object):
    def __init__(self, calendar_path: str, header: int = 0):
        if isinstance(header, int):
            calendar_df = pd.read_csv(calendar_path, dtype=str, header=header)
        else:
            calendar_df = pd.read_csv(calendar_path, dtype=str, header=None, names=["trade_date"])
        calendar_df["trade_month"] = calendar_df["trade_date"].map(lambda z: z.replace("-", "")[0:6])
        self.trade_months: list[CMonth] = []
        for trade_month, trade_month_df in calendar_df.groupby(by="trade_month"):
            month = CMonth(
                trade_month=trade_month,  # type:ignore
                trade_dates=tuple(trade_month_df["trade_date"].tolist()),
            )
            self.trade_months.append(month)

    def match_month_from_id(self, month_id: str) -> tuple[bool, CMonth | None]:
        """

        :param month_id: "YYYYMM"
        :return:
        """
        for trade_month in self.trade_months:
            if trade_month.match_from_month(month_id):
                return True, trade_month
        return False, None

    def match_month_from_date(self, trade_date: str) -> tuple[bool, CMonth | None]:
        """

        :param trade_date: "YYYYMMDD"
        :return:
        """
        for trade_month in self.trade_months:
            if trade_month.match_from_date(trade_date):
                return True, trade_month
        return False, None

    def get_next_month(self, trade_month: CMonth, shift: int) -> CMonth:
        """

        :param trade_month:
        :param shift: > 0, in the future; < 0, in the past
        :return:
        """
        trade_month_idx = self.trade_months.index(trade_month)
        return self.trade_months[trade_month_idx + shift]

    def get_iter_months(self, bgn_month: CMonth, stp_month: CMonth) -> list[CMonth]:
        bgn_idx = self.trade_months.index(bgn_month)
        stp_idx = self.trade_months.index(stp_month)
        return self.trade_months[bgn_idx:stp_idx]

    def map_iter_dates_to_iter_month(
            self, bgn_date: str, stp_date: str, calendar: CCalendar, exclude_last: bool = True
    ) -> list[CMonth]:
        """

        :param bgn_date:
        :param stp_date:
        :param calendar:
        :param exclude_last: if true, last month between iter dates is not full will be excluded from
                             the final results
        :return:
        """
        iter_dates = calendar.get_iter_list(bgn_date, stp_date)
        _, bgn_month = self.match_month_from_date(iter_dates[0])
        if exclude_last:
            true_stp_date = calendar.get_next_date(iter_dates[-1], shift=1)
            _, stp_month = self.match_month_from_date(true_stp_date)
        else:
            _, end_month = self.match_month_from_date(iter_dates[-1])
            stp_month = self.get_next_month(end_month, shift=1)
        months = self.get_iter_months(bgn_month, stp_month)
        return months

    def get_bgn_and_end_dates_for_trailing_window(self, end_month: CMonth, trn_win: int) -> tuple[str, str]:
        bgn_month = self.get_next_month(end_month, -trn_win + 1)
        bgn_date = bgn_month.bgn_date
        end_date = end_month.end_date
        return bgn_date, end_date
