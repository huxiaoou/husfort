if __name__ == "__main__":
    import argparse
    from husfort.qinstruments import CInstruMgr

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-p", "--path", required=True, help="instruments file path")
    args = arg_parser.parse_args()

    instru_mgr = CInstruMgr(instru_info_path=args.path)

    instrument = "au"
    exchange = instru_mgr.get_exchange(instrumentId=instrument)
    print(f"exchange for {instrument} is: {exchange}")

    instrument = "p"
    multiplier = instru_mgr.get_multiplier(instrumentId=instrument)
    print(f"multiplier for {instrument} is: {multiplier}")

    instrument = "CF"
    minispread = instru_mgr.get_mini_spread(instrumentId=instrument)
    print(f"minispread for {instrument} is: {minispread:.3f}")

    contract, trade_date = "ZC005", "20191201"
    new = instru_mgr.fix_contract_id(contract, trade_date=trade_date, cformat="VANILLA")
    print(f"@{trade_date} {contract} => {new}")

    contract, trade_date = "ZC105", "20200301"
    new = instru_mgr.fix_contract_id(contract, trade_date=trade_date, cformat="VANILLA")
    print(f"@{trade_date} {contract} => {new}")

    contract, trade_date = "ZC005", "20200301"
    new = instru_mgr.fix_contract_id(contract, trade_date=trade_date, cformat="VANILLA")
    print(f"@{trade_date} {contract} => {new}")

    contract, trade_date = "ZC2005", "20191201"
    new = instru_mgr.fix_contract_id(contract, trade_date=trade_date, cformat="VANILLA")
    print(f"@{trade_date} {contract} => {new}")

    contract, trade_date = "m2005", "20191201"
    new = instru_mgr.fix_contract_id(contract, trade_date=trade_date, cformat="VANILLA")
    print(f"@{trade_date} {contract} => {new}")
