#!/usr/bin/env python

if __name__ == "__main__":
    from husfort.qdataviewer import CDataViewerCSV, CArgsParserViewerCsv

    args_parser = CArgsParserViewerCsv()
    args_parser.add_arguments()
    args = args_parser.get_args()
    sort, ascending = args_parser.parse_sorts(sort=args.sort, ascending=args.ascending)

    data_viewer = CDataViewerCSV(src_path=args.path, sheet_name=args.sheet, header=args.header)
    data_viewer.fetch(cols=args.vars, where=args.where)
    data_viewer.show(
        head=args.head, tail=args.tail,
        chead=args.chead, ctail=args.ctail,
        sort=sort, ascending=ascending,
        max_rows=args.maxrows, max_cols=args.maxcols,
        transpose=args.transpose,
        precision=args.precision,
    )
    data_viewer.save(
        save_path=args.save,
        index=args.index,
        precision=args.precision,
    )
    data_viewer.pivot_table(
        pivot=args.pivot,
        values=args.values,
        columns=args.columns,
        indexes=args.indexes,
        aggfunc=args.aggfunc,
    )
