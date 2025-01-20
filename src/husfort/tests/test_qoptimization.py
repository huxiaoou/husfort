if __name__ == "__main__":
    import numpy as np
    import pandas as pd
    from husfort.qoptimization import COptimizerPortfolioUtility, COptimizerPortfolioSharpe

    m0 = np.array([0.9, 0, -0.6, 0.1])
    v0 = np.array([
        [1.1, 0.2, -0.1, 0.3],
        [0.2, 1.2, 0.15, 0.05],
        [-0.1, 0.15, 1.3, -0.2],
        [0.3, 0.05, -0.2, 1.0],
    ])
    l0 = 1
    w0 = np.array([0.2, 0.3, 0.3, 0.2])

    p = len(m0)
    bounds0 = [(1 / p / 1.5, 1.5 / p)] * p
    opt_po_ut = COptimizerPortfolioUtility(m=m0, v=v0, lbd=l0, bounds=bounds0)
    result = opt_po_ut.optimize()
    w_opt_ut = result.x

    opt_po_sr = COptimizerPortfolioSharpe(m=m0, v=v0, bounds=bounds0)
    result = opt_po_sr.optimize()
    w_opt_sr = result.x

    print("=" * 24)
    print(pd.DataFrame({"raw": w0, "opt_ut": w_opt_ut, "opt_sr": w_opt_sr}))
    print("-" * 24)
    print(f"raw Utility: {opt_po_ut.utility(w0):>9.6f}")
    print(f"opt Utility: {opt_po_ut.utility(w_opt_ut):>9.6f}")
    print(f"raw Sharpe : {opt_po_sr.sharpe(w0):>9.6f}")
    print(f"opt Sharpe : {opt_po_sr.sharpe(w_opt_sr):>9.6f}")
