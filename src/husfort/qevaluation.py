import numpy as np
import pandas as pd
from typing import Literal
from dataclasses import dataclass

'''
created @ 2024-01-05
0.  define a class to evaluate the performance of some portfolio
1.  this class provide methods to calculate some frequently used index
'''


@dataclass
class CIndicatorsGeneric:
    display_scale: int | float
    display_fmt: str  # like ".2f", "6.3f", "12d", "s"
    avlb: bool = False

    def display(self) -> str:
        raise NotImplementedError


@dataclass
class CIndicators(CIndicatorsGeneric):
    val: float | int = 0

    def display(self) -> str:
        return f"{self.val * self.display_scale:{self.display_fmt}}"


@dataclass
class CIndicatorsWithSeries(CIndicators):
    srs: pd.Series = None
    idx: int | str = None

    def displayIdx(self) -> str:
        if isinstance(self.idx, int):
            return f"{self.idx:d}"
        elif isinstance(self.idx, str):
            return f"{self.idx:s}"
        else:
            raise ValueError(f"wrong type for self.idx")


@dataclass
class CIndicatorsWithDict(CIndicatorsGeneric):
    val: dict[str, float] = None

    def display(self) -> str:
        return ",".join([f"{k}={v * self.display_scale:{self.display_fmt}}" for k, v in self.val.items()])


class CNAV(object):
    def __init__(
            self, input_srs: pd.Series, input_type: Literal["NAV", "RET"],
            annual_factor: float = 250, annual_rf_rate: float = 0, ret_scale_display: float = 100,
    ):
        """

        :param input_srs: A. if input_type == "NAV":
                                    the Net-Assets-Value series, with datetime-like index in string format.
                                    The first item in this series COULD NOT be 1, and the class will do the conversion
                                    when initialized.
                                 elif input_type == "RET":
                                    the Assets Return series, the return should NOT be multiplied by RETURN_SCALE
                              B. the index of the series is supposed to be continuous, i.e., there are not any missing
                                 dates or timestamp in the index.
        :param input_type: "NAV" or "RET"
        :param annual_rf_rate: annualized risk-free rate, must NOT be multiplied by the return scale.
                               the class will do the conversion when initialized
        :param annual_factor: if the return series means a return with hold period = T trading days,
                              this value should by 250/T
                                                  |  T | annual factor |               |
                                daily     returns |  1 |      250      | default value |
                                weekly    returns |  5 |       50      |               |
                                monthly   returns | 21 |       12      |               |
                                quarterly returns | 63 |        4      |               |
        """
        self.return_type = input_type.upper()
        self.annual_factor = annual_factor
        self.annual_rf_rate: float = annual_rf_rate
        self.ret_scale_display: float = ret_scale_display

        if self.return_type == "NAV":
            self.nav_srs: pd.Series = input_srs / input_srs.iloc[0]  # always set the first value to be 1
            self.rtn_srs: pd.Series = (input_srs / input_srs.shift(1) - 1).fillna(0)  # has the same length as nav srs
        elif self.return_type == "RET":
            self.rtn_srs: pd.Series = input_srs
            self.nav_srs: pd.Series = (input_srs + 1).cumprod()
        else:
            raise ValueError(f"input type = {input_type} is illegal, please check again.")

        self.obs: int = len(input_srs)

        # frequently used performance indicators
        # primary
        self.return_mean: CIndicators = CIndicators(ret_scale_display, display_fmt=".3f")
        self.return_std: CIndicators = CIndicators(ret_scale_display, display_fmt=".3f")
        self.hold_period_return: CIndicators = CIndicators(ret_scale_display, display_fmt=".2f")
        self.annual_return: CIndicators = CIndicators(ret_scale_display, display_fmt=".2f")
        self.annual_volatility: CIndicators = CIndicators(ret_scale_display, display_fmt=".2f")
        self.sharpe_ratio: CIndicators = CIndicators(1, display_fmt=".3f")
        self.calmar_ratio: CIndicators = CIndicators(1, display_fmt=".3f")
        self.score: CIndicators = CIndicators(1, display_fmt=".3f")
        self.value_at_risks: CIndicatorsWithDict = CIndicatorsWithDict(ret_scale_display, display_fmt=".3f")

        # secondary
        self.max_drawdown_scale: CIndicatorsWithSeries = CIndicatorsWithSeries(
            display_scale=ret_scale_display, display_fmt=".3f", srs=pd.Series(index=self.nav_srs.index),
        )
        self.longest_drawdown_duration: CIndicatorsWithSeries = CIndicatorsWithSeries(
            display_scale=1, display_fmt="d", srs=pd.Series(data=0, index=self.nav_srs.index),
        )
        self.longest_recover_duration: CIndicatorsWithSeries = CIndicatorsWithSeries(
            display_scale=1, display_fmt="d", srs=pd.Series(data=0, index=self.nav_srs.index),
        )

    def cal_return_mean(self):
        if not self.return_mean.avlb:
            self.return_mean.val = self.rtn_srs.mean()
            self.return_mean.avlb = True
        return 0

    def cal_return_std(self):
        if not self.return_std.avlb:
            self.return_std.val = self.rtn_srs.std()
            self.return_std.avlb = True
        return 0

    def cal_hold_period_return(self):
        if not self.hold_period_return.avlb:
            self.hold_period_return.val = self.nav_srs.iloc[-1] - 1
            self.hold_period_return.avlb = True
        return 0

    def cal_annual_return(self, method: str = "linear"):
        if not self.annual_return.avlb:
            if method.lower() == "linear":
                self.annual_return.val = self.rtn_srs.mean() * self.annual_factor
            elif method.lower() == "compound":
                self.annual_return.val = np.power(self.nav_srs.iloc[-1], self.annual_factor / self.obs) - 1
            else:
                raise ValueError(f"method = {method} is not a legal option")
            self.annual_return.avlb = True
        return 0

    def cal_annual_volatility(self):
        if not self.annual_volatility.avlb:
            self.annual_volatility.val = self.rtn_srs.std() * np.sqrt(self.annual_factor)
            self.annual_volatility.avlb = True
        return 0

    def cal_sharpe_ratio(self):
        if not self.sharpe_ratio.avlb:
            diff_srs = self.rtn_srs - self.annual_rf_rate / self.annual_factor
            mu = diff_srs.mean()
            sd = diff_srs.std()
            self.sharpe_ratio.val = mu / sd * np.sqrt(self.annual_factor)
            self.sharpe_ratio.avlb = True
        return 0

    def cal_max_drawdown_scale(self):
        if not self.max_drawdown_scale.avlb:
            self.max_drawdown_scale.srs = (1 - self.nav_srs / self.nav_srs.cummax())
            self.max_drawdown_scale.val = self.max_drawdown_scale.srs.max()
            self.max_drawdown_scale.idx = self.max_drawdown_scale.srs.idxmax()
            self.max_drawdown_scale.avlb = True
        return 0

    def cal_calmar_ratio(self):
        if not self.calmar_ratio.avlb:
            self.cal_annual_return()
            self.cal_max_drawdown_scale()
            self.calmar_ratio.val = self.annual_return.val / self.max_drawdown_scale.val
            self.calmar_ratio.avlb = True
        return 0

    def cal_score(self):
        if not self.score.avlb:
            if self.sharpe_ratio.avlb and self.calmar_ratio.avlb:
                self.score.val = self.sharpe_ratio.val + self.calmar_ratio.val
                self.score.avlb = True
            else:
                print(f"Sharpe ratio is {'' if self.sharpe_ratio.avlb else 'not '}available")
                print(f"Calmar ratio is {'' if self.calmar_ratio.avlb else 'not '}available")
        return 0

    def cal_longest_drawdown_duration(self):
        if self.longest_drawdown_duration.avlb:
            return 0
        prev_high = self.nav_srs.iloc[0]
        prev_high_loc = 0
        prev_drawdown_scale = 0.0
        drawdown_loc = 0
        for i, nav_i in enumerate(self.nav_srs):
            if nav_i > prev_high:
                prev_high = nav_i
                prev_high_loc = i
                prev_drawdown_scale = 0
            drawdown_scale = 1 - nav_i / prev_high  # type:ignore
            if drawdown_scale > prev_drawdown_scale:
                prev_drawdown_scale = drawdown_scale
                drawdown_loc = i
            self.longest_drawdown_duration.srs.iloc[i] = drawdown_loc - prev_high_loc
        self.longest_drawdown_duration.val = self.longest_drawdown_duration.srs.max()
        self.longest_drawdown_duration.idx = self.longest_drawdown_duration.srs.idxmax()
        self.longest_drawdown_duration.avlb = True
        return 0

    def cal_longest_recover_duration(self):
        if self.longest_recover_duration.avlb:
            return 0
        prev_high = self.nav_srs.iloc[0]
        prev_high_loc = 0
        for i, nav_i in enumerate(self.nav_srs):
            if nav_i > prev_high:
                self.longest_recover_duration.srs.iloc[i] = 0
                prev_high = nav_i
                prev_high_loc = i
            else:
                self.longest_recover_duration.srs.iloc[i] = i - prev_high_loc
        self.longest_recover_duration.val = self.longest_recover_duration.srs.max()
        self.longest_recover_duration.idx = self.longest_recover_duration.srs.idxmax()
        self.longest_recover_duration.avlb = True
        return 0

    def cal_value_at_risk(self, qs: tuple[int, ...]):
        if (not self.value_at_risks.avlb) and qs:
            self.value_at_risks.val = {f"q{q:02d}": np.percentile(self.rtn_srs, q) for q in qs}
            self.value_at_risks.avlb = True
        return 0

    def cal_all_indicators(self, method: str = "linear",
                           excluded: tuple[str, ...] = (),
                           qs: tuple[int, ...] = ()):
        """

        :param method: "linear" or "compound"
        :param excluded: indicators in this tuple will not be calculated, only
                         ("ldd", "lrd", "var")
                         can be excluded, using this to save time when
        :param qs: Percentage or sequence of percentages for the percentiles to compute.
                   Values must be between 0 and 100 inclusive.
                   This parameter must be provided if user want to calculate the indicator 'VaR',
                   In other words, if 'var' not in parameter 'excluded', this must be provided.
        :return:
        """
        self.cal_return_mean()
        self.cal_return_std()
        self.cal_hold_period_return()
        self.cal_annual_return(method=method)
        self.cal_annual_volatility()
        self.cal_sharpe_ratio()
        self.cal_max_drawdown_scale()
        self.cal_calmar_ratio()
        self.cal_score()

        if "ldd" not in excluded:
            self.cal_longest_drawdown_duration()
        if "lrd" not in excluded:
            self.cal_longest_recover_duration()
        if "var" not in excluded:
            self.cal_value_at_risk(qs=qs)
        return 0

    def to_dict(self) -> dict:
        d = {}
        if self.return_mean.avlb:
            d.update({"retMean": self.return_mean.val})

        if self.return_std.avlb:
            d.update({"retStd": self.return_std.val})

        if self.hold_period_return.avlb:
            d.update({"hpr": self.hold_period_return.val})

        if self.annual_return.avlb:
            d.update({"retAnnual": self.annual_return.val})

        if self.annual_volatility.avlb:
            d.update({"volAnnual": self.annual_volatility.val})

        if self.sharpe_ratio.avlb:
            d.update({"sharpe": self.sharpe_ratio.val})

        if self.calmar_ratio.avlb:
            d.update({"calmar": self.calmar_ratio.val})

        if self.score.avlb:
            d.update({"score": self.score.val})

        if self.max_drawdown_scale.avlb:
            d.update({
                "mdd": self.max_drawdown_scale.val,
                "mddT": self.max_drawdown_scale.idx,
            })

        if self.longest_drawdown_duration.avlb:
            d.update({
                "lddDur": self.longest_drawdown_duration.val,
                "lddDurT": self.longest_drawdown_duration.idx,
            })

        if self.longest_recover_duration.avlb:
            d.update({
                "lrd": self.longest_recover_duration.val,
                "lrdT": self.longest_recover_duration.idx,
            })

        if self.value_at_risks.avlb:
            d.update(self.value_at_risks.val)
        return d

    def reformat_to_display(self) -> dict:
        d = {}
        if self.return_mean.avlb:
            d.update({"retMean": self.return_mean.display()})

        if self.return_std.avlb:
            d.update({"retStd": self.return_std.display()})

        if self.hold_period_return.avlb:
            d.update({"hpr": self.hold_period_return.display()})

        if self.annual_return.avlb:
            d.update({"retAnnual": self.annual_return.display()})

        if self.annual_volatility.avlb:
            d.update({"volAnnual": self.annual_volatility.display()})

        if self.sharpe_ratio.avlb:
            d.update({"sharpe": self.sharpe_ratio.display()})

        if self.calmar_ratio.avlb:
            d.update({"calmar": self.calmar_ratio.display()})

        if self.score.avlb:
            d.update({"score": self.score.display()})

        if self.max_drawdown_scale.avlb:
            d.update({
                "mdd": self.max_drawdown_scale.display(),
                "mddT": self.max_drawdown_scale.displayIdx(),
            })

        if self.longest_drawdown_duration.avlb:
            d.update({
                "lddDur": self.longest_drawdown_duration.display(),
                "lddDurT": self.longest_drawdown_duration.displayIdx(),
            })

        if self.longest_recover_duration.avlb:
            d.update({
                "lrd": self.longest_recover_duration.display(),
                "lrdT": self.longest_recover_duration.displayIdx(),
            })

        if self.value_at_risks.avlb:
            s = self.value_at_risks.display()
            for val in s.split(","):
                k, v = val.split("=")
                d[k] = v
        return d
