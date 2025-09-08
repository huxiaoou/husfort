import warnings
import numpy as np
import numpy.typing as npt
from typing import Union
from scipy.optimize import minimize, NonlinearConstraint, OptimizeResult
from numba import njit


@njit(cache=True)
def returns(w: npt.NDArray, m: npt.NDArray) -> npt.NDArray:
    return w @ m


@njit(cache=True)
def covariance(w: npt.NDArray, v: npt.NDArray) -> npt.NDArray:
    return w @ v @ w


@njit(cache=True)
def volatility(w: npt.NDArray, v: npt.NDArray) -> npt.NDArray:
    return np.power(w @ v @ w, 0.5)


@njit(cache=True)
def sharpe(w: npt.NDArray, m: npt.NDArray, v: npt.NDArray) -> npt.NDArray:
    return returns(w, m) / volatility(w, v)


@njit(cache=True)
def utility(w: npt.NDArray, m: npt.NDArray, v: npt.NDArray, lbd: float) -> npt.NDArray:
    return returns(w, m) - 0.5 * lbd * covariance(w, v)


@njit(cache=True)
def jac_sharpe(w: npt.NDArray, m: npt.NDArray, v: npt.NDArray) -> npt.NDArray:
    return (m - sharpe(w, m, v) * (v @ w)) / covariance(w, v)


class COptimizerPortfolio:
    def __init__(self, m: np.ndarray, v: np.ndarray, verbose: bool = True, ignore_warnings: bool = False):
        """

        :param m: mean matrix with size = p x 1
        :param v: covariance matrix with size = p x p
        """
        self.m = m
        self.v = v
        self.verbose = verbose
        self.ignore_warnings = ignore_warnings
        self.p, _ = self.v.shape
        if self.p != _:
            raise ValueError(f"Shape of covariance is = ({self.v.shape})")
        if self.p != len(self.m):
            raise ValueError(f"Shape of mean is = {self.m.shape}, covariance is = {self.v.shape}, not aligned")

    @property
    def variable_n(self) -> int:
        return self.p

    def returns(self, w: np.ndarray):
        return returns(w=w, m=self.m)

    def covariance(self, w: np.ndarray):
        return covariance(w=w, v=self.v)

    def volatility(self, w: np.ndarray):
        return volatility(w=w, v=self.v)

    def sharpe(self, w: np.ndarray):
        return sharpe(w=w, m=self.m, v=self.v)

    def target(self, w: np.ndarray):
        raise NotImplementedError

    def optimize(self) -> tuple[np.ndarray, float]:
        raise NotImplementedError

    @staticmethod
    def parse_res(func):
        def parse_res_for_fun(self, **kwargs):
            res = func(self, **kwargs)
            if not res.success and self.verbose:
                print("ERROR! Optimizer exits with a failure")
                print(f"Detailed Description: {res.message}")
            return res

        return parse_res_for_fun

    def turn_off_warnings(self):
        if self.ignore_warnings:
            warnings.filterwarnings(
                "ignore",
                message="Values in x were outside bounds during a minimize step, clipping to bounds",
            )


class _COptimizerScipyMinimize(COptimizerPortfolio):
    def __init__(
            self,
            m: np.ndarray,
            v: np.ndarray,
            x0: Union[np.ndarray, str],
            max_iter: int,
            tol: float,
            verbose: bool = True,
            ignore_warnings: bool = False,
    ):
        """

        :param x0: init guess, or a string to indicate the method to generate init guess, available
                   options = ("aver", )
        :param max_iter: maximum iteration
        :param tol: when too small, the target may be sensitive to the change of the input, and
                    results maybe overfitted. You may want to adjust it to adapt to your input
                    data scale.
        """
        super().__init__(m, v, verbose=verbose, ignore_warnings=ignore_warnings)
        if isinstance(x0, str):
            if x0 == "aver":
                self.x0 = np.ones(self.p) / self.p
            else:
                raise ValueError(f"x0 = {x0} is illegal, try numpy array or literal string, such as 'aver'")
        else:
            self.x0 = x0
        self.max_iter = max_iter
        self.tol = tol


class COptimizerPortfolioUtility(_COptimizerScipyMinimize):
    def __init__(
            self,
            m: np.ndarray, v: np.ndarray, lbd: float, x0: Union[np.ndarray, str],
            tot_mkt_val_bds: tuple[float, float] = (0.0, 1.0),
            bounds: list[tuple[float, float]] = None,
            max_iter: int = 50000,
            tol: float = 1e-6,
            verbose: bool = True,
            ignore_warnings: bool = False,
    ):
        """

        :param lbd:
        :param tot_mkt_val_bds: lb <= sum(abs(w)) <= ub, we highly suggest using lb = 0, or
                                 the problem will not be convex
        :param bounds: bounds[0] <= w_i <= bounds[1], Sequence of (min, max) pairs for each
                       element in x. None is used to specify no bound.
        :return:
        """

        super().__init__(m, v, x0, max_iter, tol, verbose=verbose, ignore_warnings=ignore_warnings)
        self.lbd = lbd
        self.tot_mkt_val_bds = tot_mkt_val_bds
        self.bounds = bounds

    def utility(self, w: np.ndarray):
        return utility(w=w, m=self.m, v=self.v, lbd=self.lbd)

    def target(self, w: np.ndarray):
        return -self.utility(w)

    @COptimizerPortfolio.parse_res
    def optimize(self) -> OptimizeResult:
        self.turn_off_warnings()
        lb, ub = self.tot_mkt_val_bds
        cons = NonlinearConstraint(lambda z: np.sum(np.abs(z)), lb=lb, ub=ub)  # control total market value

        # noinspection PyTypeChecker
        res = minimize(
            fun=self.target, x0=self.x0,
            bounds=self.bounds,
            constraints=[cons],
            options={"maxiter": self.max_iter},
            tol=self.tol,
        )
        return res


class COptimizerPortfolioSharpe(_COptimizerScipyMinimize):
    def __init__(
            self,
            m: np.ndarray, v: np.ndarray, x0: Union[np.ndarray, str],
            bounds: list[tuple[float, float]],
            tot_mkt_val_bds: tuple[float, float] = (0.0, 1.0),
            max_iter: int = 50000,
            tol: float = 1e-6,
            using_jac: bool = False,
            verbose: bool = True,
            ignore_warnings: bool = False,
    ):
        """

        :param bounds: bounds[0] <= w_i <= bounds[1], Sequence of (min, max) pairs for each
                       element in x. None is used to specify no bound.
        :param tot_mkt_val_bds: theoretically, sharpe ratio is irrelevant to this bounds
                                user may ignore it. However, this may affect the result in practise.
        :param max_iter: maximum iteration
        :param using_jac: whether to use Jacobian matrix, this may accelerate the speed
        """
        super().__init__(m, v, x0, max_iter, tol, verbose=verbose, ignore_warnings=ignore_warnings)
        self.bounds = bounds
        self.tot_mkt_val_bds = tot_mkt_val_bds
        self.using_jac = using_jac

    def target(self, w: np.ndarray):
        return -self.sharpe(w)

    def jac(self, w: np.ndarray):
        return -jac_sharpe(w=w, m=self.m, v=self.v)

    @COptimizerPortfolio.parse_res
    def optimize(self) -> OptimizeResult:
        self.turn_off_warnings()
        # sharpe ratio is irrelevant to ths scale of z, i.e. the total market value
        # as the result of this, we provide a FIX scope for it
        lb, ub = self.tot_mkt_val_bds
        cons = NonlinearConstraint(lambda z: np.sum(np.abs(z)), lb=lb, ub=ub)

        # noinspection PyTypeChecker
        res = minimize(
            fun=self.target, x0=self.x0,
            bounds=self.bounds,
            constraints=[cons],
            options={"maxiter": self.max_iter},
            tol=self.tol,
            jac=self.jac if self.using_jac else None,
        )
        return res
