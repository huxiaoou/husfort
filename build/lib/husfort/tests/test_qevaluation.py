if __name__ == "__main__":
    import pandas as pd
    import scipy.stats as sps
    from husfort.qevaluation import CNAV

    mu, sd = 0.002, 0.012
    n = 2000
    dates = [f"T{_:04d}" for _ in range(n)]
    ret = sps.norm.rvs(loc=mu, scale=sd, size=n)

    ret_data = pd.Series(data=ret, index=dates)
    print(ret_data)

    nav = CNAV(input_srs=ret_data, input_type="RET")
    nav.cal_all_indicators(excluded=("var", ), qs=(1, 99))
    res = nav.to_dict()
    print(res)

    res_display = nav.reformat_to_display()
    print(res_display)
