import numpy as np
from scipy.optimize import minimize, NonlinearConstraint, OptimizeResult


class COptimizerPortfolio:
    def __init__(self, m: np.ndarray, v: np.ndarray):
        """

        :param m: mean matrix with size = p x 1
        :param v: covariance matrix with size = p x p
        """
        self.m = m
        self.v = v
        self.p, _ = self.v.shape
        if self.p != _:
            raise ValueError(f"Shape of covariance is = ({self.v.shape})")
        if self.p != len(self.m):
            raise ValueError(f"Shape of mean is = {self.m.shape}, covariance is = {self.v.shape}, not aligned")

    @property
    def variable_n(self) -> int:
        return self.p

    def returns(self, w: np.ndarray):
        return w @ self.m

    def covariance(self, w: np.ndarray):
        return w @ self.v @ w

    def volatility(self, w: np.ndarray):
        return self.covariance(w) ** 0.5

    def sharpe(self, w: np.ndarray):
        return self.returns(w) / self.volatility(w)

    def target(self, w: np.ndarray):
        raise NotImplementedError

    def optimize(self) -> tuple[np.ndarray, float]:
        raise NotImplementedError

    @staticmethod
    def parse_res(func):
        def parse_res_for_fun(self, **kwargs):
            res = func(self, **kwargs)
            if not res.success:
                print("ERROR! Optimizer exits with a failure")
                print(f"Detailed Description: {res.message}")
            return res

        return parse_res_for_fun


class COptimizerPortfolioUtility(COptimizerPortfolio):
    def __init__(
            self,
            m: np.ndarray, v: np.ndarray,
            lbd: float,
            tot_mkt_val_bds: tuple[float, float] = (0.0, 1.0),
            bounds: list[tuple[float, float]] = None,
            max_iter: int = 50000
    ):
        """
            :param lbd:
            :param tot_mkt_val_bds: lb <= sum(abs(w)) <= ub, we highly suggest using lb = 0, or
                                     the problem will not be convex
            :param bounds: bounds[0] <= w_i <= bounds[1], Sequence of (min, max) pairs for each
                           element in x. None is used to specify no bound.
            :param max_iter:
            :return:
        """

        super().__init__(m=m, v=v)
        self.lbd = lbd
        self.tot_mkt_val_bds = tot_mkt_val_bds
        self.bounds = bounds
        self.max_iter = max_iter

    def utility(self, w: np.ndarray):
        return self.returns(w) - 0.5 * self.lbd * self.covariance(w)

    def target(self, w: np.ndarray):
        return -self.utility(w)

    @COptimizerPortfolio.parse_res
    def optimize(self) -> OptimizeResult:
        lb, ub = self.tot_mkt_val_bds
        cons = NonlinearConstraint(lambda z: np.sum(np.abs(z)), lb=lb, ub=ub)  # control total market value

        # noinspection PyTypeChecker
        res = minimize(
            fun=self.target, x0=np.ones(self.p) / self.p,
            bounds=self.bounds,
            constraints=[cons],
            options={"maxiter": self.max_iter}
        )
        return res


class COptimizerPortfolioSharpe(COptimizerPortfolio):
    def __init__(
            self,
            m: np.ndarray, v: np.ndarray,
            bounds: list[tuple[float, float]],
            max_iter: int = 50000
    ):
        super().__init__(m=m, v=v)
        self.bounds = bounds
        self.max_iter = max_iter

    def target(self, w: np.ndarray):
        return -self.sharpe(w)

    @COptimizerPortfolio.parse_res
    def optimize(self) -> OptimizeResult:
        # sharpe ratio is irrelevant to ths scale of z, i.e. the total market value
        # as the result of this, we provide a FIX scope for it
        cons = NonlinearConstraint(lambda z: np.sum(np.abs(z)), lb=0.0, ub=1.0)

        # noinspection PyTypeChecker
        res = minimize(
            fun=self.target, x0=np.ones(self.p) / self.p,
            bounds=self.bounds,
            constraints=[cons],
            options={"maxiter": self.max_iter}
        )
        return res
