import numpy as np
import pandas as pd
from scipy.optimize import minimize, NonlinearConstraint


class DimError(ValueError):
    pass


class COptimizerPortfolio(object):
    def __init__(self, m: np.ndarray, v: np.ndarray):
        """

        :param m: mean matrix with size = p x 1
        :param v: covariance matrix with size = p x p
        """
        self.m = m
        self.v = v
        self.p, _ = self.v.shape
        if self.p != _:
            print(f"... Shape of covariance is = ({self.p}, {_})")
            raise DimError
        if self.p != len(self.m):
            print(f"... Shape of mean is       = ({len(self.m)}, 1)")
            print(f"... Shape of covariance is = ({self.p}, {_})")
            raise DimError

    @property
    def variable_n(self) -> int:
        return self.p

    def returns(self, w: np.ndarray):
        return w @ self.m

    def covariance(self, w: np.ndarray):
        return w @ self.v @ w

    def volatility(self, w: np.ndarray):
        return self.covariance(w) ** 0.5

    def utility(self, w: np.ndarray, lbd: float):
        return -self.returns(w) + 0.5 * lbd * self.covariance(w)

    def sharpe(self, w: np.ndarray):
        return self.returns(w) / self.volatility(w)

    def utility_sharpe(self, w: np.ndarray):
        return -self.sharpe(w)

    @staticmethod
    def _parse_res(func):
        def parse_res_for_fun(self, **kwargs):
            res = func(self, **kwargs)
            if res.success:
                return res.x, res.fun
            else:
                print("ERROR! Optimizer exits with a failure")
                print(f"Detailed Description: {res.message}")
                return None, None

        return parse_res_for_fun

    @_parse_res
    def optimize_utility(self, lbd: float, tot_mkt_val_bds: tuple[float, float] = (0.0, 1.0),
                         bounds: list[tuple[float, float]] = None,
                         max_iter: int = 50000):
        """

        :param lbd:
        :param tot_mkt_val_bds: lb <= sum(abs(w)) <= ub, we highly suggest using lb = 0, or
                                 the problem will not be convex
        :param bounds: bounds[0] <= w_i <= bounds[1], Sequence of (min, max) pairs for each
                       element in x. None is used to specify no bound.
        :param max_iter:
        :return:
        """
        lb, ub = tot_mkt_val_bds
        cons = NonlinearConstraint(lambda z: np.sum(np.abs(z)), lb=lb, ub=ub)  # control total market value
        # noinspection PyTypeChecker
        res = minimize(
            fun=self.utility, x0=np.ones(self.p) / self.p,
            args=(lbd,),
            bounds=bounds,
            constraints=[cons],
            options={"maxiter": max_iter}
        )
        return res

    @_parse_res
    def optimize_sharpe(self, bounds: list[tuple[float, float]], max_iter: int = 50000):
        # sharpe ratio is irrelevant to ths scale of z, i.e. the total market value
        # as the result of this, we provide a FIX scope for it
        cons = NonlinearConstraint(lambda z: np.sum(np.abs(z)), lb=0.0, ub=1.0)
        # noinspection PyTypeChecker
        res = minimize(
            fun=self.utility_sharpe, x0=np.ones(self.p) / self.p,
            bounds=bounds,
            constraints=[cons],
            options={"maxiter": max_iter}
        )
        return res


if __name__ == "__main__":
    import sys

    m0 = np.array([0.9, 0, -0.6, 0.1])
    v0 = np.array([
        [1.1, 0.2, -0.1, 0.3],
        [0.2, 1.2, 0.15, 0.05],
        [-0.1, 0.15, 1.3, -0.2],
        [0.3, 0.05, -0.2, 1.0],
    ])
    l0 = float(sys.argv[1])
    w0 = np.array([0.2, 0.3, 0.3, 0.2])

    opt_po = COptimizerPortfolio(m=m0, v=v0)
    p = opt_po.variable_n
    bounds0 = [(1 / p / 1.5, 1.5 / p)] * p
    w_opt_ut, _ = opt_po.optimize_utility(lbd=l0, bounds=bounds0)
    w_opt_sr, _ = opt_po.optimize_sharpe(bounds=bounds0)

    print("=" * 24)
    print(pd.DataFrame({"raw": w0, "opt_ut": w_opt_ut, "opt_sr": w_opt_sr}))
    print("-" * 24)
    print(f"raw Sharpe : {opt_po.sharpe(w0):>9.6f}")
    print(f"opt Sharpe : {opt_po.sharpe(w_opt_sr):>9.6f}")
    print(f"raw Utility: {opt_po.utility(w0, l0):>9.6f}")
    print(f"opt Utility: {opt_po.utility(w_opt_ut, l0):>9.6f}")
