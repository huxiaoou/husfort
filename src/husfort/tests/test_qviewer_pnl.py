from husfort.qviewer_pnl import CManagerViewer, CPosition, CContract, CCfg, CAccountTianqin


def gen_positions(pos: str) -> list[CPosition]:
    pos_data = pd.read_csv(io.StringIO(pos.replace("\\n", "\n")), sep=",", header=None, names=names)
    positions: list[CPosition] = []
    for idx, r in pos_data.iterrows():
        pos = CPosition(
            contract=CContract(
                contractId=r["contract"],
                instrumentId=r["instrument"],
                exchangeId=r["exchange"],
                multiplier=r["multiplier"],
            ),
            direction=r["direction"],
            qty=r["qty"],
            base_price=r["base_price"],
        )
        positions.append(pos)
    return positions


if __name__ == "__main__":
    import argparse
    import pandas as pd
    import io

    names = ["contract", "instrument", "exchange", "multiplier", "direction", "qty", "base_price"]
    default_pos = r"y2505,y,DCE,10,1,20,7944\nrb2505,rb,SHFE,10,-1,3,3337\nCF505,CF,CZCE,5,1,4,13850"
    help_pos = f"a string for positions, like '{default_pos}', columns must be: {names}"

    arg_parser = argparse.ArgumentParser(
        description="a viewer to view float pnl for given positions.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    arg_parser.add_argument("--account", type=str, required=True, help="tianqin account")
    arg_parser.add_argument("--password", type=str, required=True, help="tianqin account password")
    arg_parser.add_argument("--pos", type=str, help=help_pos, default=default_pos)
    args = arg_parser.parse_args()

    config = CCfg(account=CAccountTianqin(args.account, args.password))
    mgr = CManagerViewer(positions=gen_positions(args.pos), config=config)
    mgr.main()
