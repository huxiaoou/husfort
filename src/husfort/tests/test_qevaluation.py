if __name__ == "__main__":
    import argparse
    import pandas as pd
    import scipy.stats as sps
    from husfort.qevaluation import CNAV

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "-m", "--mu", type=float, default=0.002, help="average of return")
    arg_parser.add_argument(
        "-s", "--sigma", type=float, default=0.012, help="volatility of return")
    arg_parser.add_argument(
        "--size", type=int, default=2000, help="size of samples"
    )
    args = arg_parser.parse_args()

    mu, sd = args.mu, args.sigma
    n = args.size
    dates = [f"T{_:04d}" for _ in range(n)]
    ret = sps.norm.rvs(loc=mu, scale=sd, size=n)

    ret_data = pd.Series(data=ret, index=dates)
    print(ret_data)
    print("-" * 24)

    nav = CNAV(input_srs=ret_data, input_type="RET")
    nav.cal_all_indicators(excluded=("var",), qs=(1, 99))
    res = nav.to_dict()
    print(pd.Series(res))
    print("-" * 24)

    res_display = nav.reformat_to_display()
    print(pd.Series(res_display))
    print("-" * 24)
