import argparse
import pandas as pd
import os.path
import datetime
import sys

sys.path.insert(0, os.path.abspath('..'))

import limnpy

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('csv', help='name of file to be transformed')
    parser.add_argument('--delim', default='\t', help='delim to use for input csv')
    parser.add_argument('--header', nargs='+', help='')

    parser.add_argument('--pivot', default=False, action='store_true', help='whether to try and automatically pivot the data')
    parser.add_argument('--datecol', type=int, default=0, help='the date column')
    parser.add_argument('--datefmt', help='format to use with datetime.strptime')
    parser.add_argument('--metriccol', default=1, help='the column to use for creating distinct metrics')
    parser.add_argument('--valcol', default=2, help='the column to use for the actual data to be plotted')

    parser.add_argument('--basedir', default='.', help='directory in which to place the datasources, datafiles and graphs directories')
    parser.add_argument('--name', nargs='+', type=' '.join, help='name of datasource')
    parser.add_argument('--id', help='datasource id')
    parser.add_argument('--write_graph', default=False, help='whether to write the graph')

    args = parser.parse_args()

    date_parser = lambda s : datetime.datetime.strptime(s, args.datefmt)

    if not args.pivot:
        df = pd.read_table(args.csv, sep=args.delim, parse_dates=[args.datecol], date_parser=date_parser)
    else:
        df_long = pd.read_table(args.csv, sep=args.delim, parse_dates=[args.datecol], date_parser=date_parser)

        if isinstance(args.datecol, int):
            args.datecol = df_long.columns[args.datecol]
        if isinstance(args.metriccol, int):
            args.metriccol = df_long.columns[args.metriccol]
        if isinstance(args.valcol, int):
            args.valcol = df_long.columns[args.valcol]

        df = df_long.pivot(index=args.datecol, columns=args.metriccol, values=args.valcol)


    print df

    if args.name is None:
        args.name = os.path.splitext(args.csv)[0]
    if args.id is None:
        args.id = os.path.splitext(args.csv)[0]
    ds = limnpy.DataSource(args.id, args.name, df)
    ds.write(args.basedir)

    if args.write_graph:
        graph = ds.get_graph()
        graph.write(args.basedir)

if __name__ == '__main__':
    main()
