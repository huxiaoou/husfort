#!/usr/bin/env python


if __name__ == "__main__":
    from husfort.qdataviewer import CDataViewerH5, CArgsParserViewerH5

    args_parser = CArgsParserViewerH5()
    args_parser.add_arguments()
    args = args_parser.get_args()

    data_viewer = CDataViewerH5(lib=args.lib, table=args.table)
    cols = args.vars.split(",") if args.vars else []
    data_viewer.fetch(cols=cols, where=args.where)
    data_viewer.show(
        head=args.head, tail=args.tail,
        chead=args.chead, ctail=args.ctail,
        max_rows=args.maxrows, max_cols=args.maxcols,
        transpose=args.transpose,
    )
    data_viewer.save(
        save_path=args.save,
        index=args.index,
        float_format=args.floatfmt,
    )
