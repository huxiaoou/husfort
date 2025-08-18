#!/usr/bin/env python

if __name__ == "__main__":
    from husfort.qdataviewer import CDataViewerCSV, CArgsParserViewerCsv

    args_parser = CArgsParserViewerCsv()
    args_parser.add_arguments()
    args = args_parser.get_args()
    cols = args_parser.parse_vars(variables=args.vars)
    sort, ascending = args_parser.parse_sorts(sort=args.sort, ascending=args.ascending)

    data_viewer = CDataViewerCSV(src_path=args.path, sheet_name=args.sheet, header=args.header)
    data_viewer.fetch(cols=cols, where=args.where)
    data_viewer.show(
        head=args.head, tail=args.tail,
        chead=args.chead, ctail=args.ctail,
        sort=sort, ascending=ascending,
        max_rows=args.maxrows, max_cols=args.maxcols,
        transpose=args.transpose,
    )
    data_viewer.save(
        save_path=args.save,
        index=args.index,
        float_format=args.floatfmt,
    )
