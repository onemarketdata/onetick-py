import argparse
import os
import sys
from pathlib import Path
from textwrap import dedent

import pandas as pd

import onetick.py as otp
from onetick.py.otq import otq


def parser_impl(parser: argparse.ArgumentParser):
    parser.formatter_class = argparse.RawDescriptionHelpFormatter
    parser.description = dedent("""
        *%(prog)s* reads OneTick databases.
        It uses onetick-py to set-up OneTick configuration, authenticate and run the query.

        Database data will be printed to the standard output.
        If specified database does not exist, error will be printed to the standard error stream.
        If there is no data, or the specified tick type or symbol do not exist, nothing will be printed.

        Example:

        $ %(prog)s US_COMP_SAMPLE QTE AAPL '2024-02-01 09:30:00' '2024-02-01 09:30:01' --tz America/New_York --limit 100 --columns-regex PRICE

        Several ways of configuration are possible:

        1. Use ONE_TICK_CONFIG environment variable:

            export ONE_TICK_CONFIG=/path/to/one_tick_config.txt
            %(prog)s ...

        2. Use onetick-py environment variables, e.g. using WebAPI mode and OneTick Cloud:

            export OTP_WEBAPI='1'
            export OTP_HTTP_ADDRESS='https://rest.cloud.onetick.com'
            export OTP_ACCESS_TOKEN_URL='https://cloud-auth.parent.onetick.com/realms/OMD/protocol/openid-connect/token'
            export OTP_CLIENT_ID='???????????'
            export OTP_CLIENT_SECRET='???????'
            %(prog)s ...

        3. Using --remote-ts parameter to connect to remote OneTick server:

            %(prog)s ... --remote-ts path.to.remote.onetick.com:50015

        4. Reading local OneTick database directory:

           %(prog)s ./path/to/local/DB ...
    """)

    db_group = parser.add_argument_group('Database')
    db_group.add_argument('db', help='Database name or path to the local directory.')
    db_group.add_argument('tt', help='Tick type.')
    db_group.add_argument('symbol', help='Symbol name.')

    time_group = parser.add_argument_group(
        'Time range',
        description=dedent("""
            Supported datetime formats: ISO 8601
                2024-02-01
                2024-02-01 09:30
                2024-02-01 09:30:01
                2024-02-01 09:30:01.499
                2024-02-01 09:30:01.499865
                2024-02-01 09:30:01.499865744
        """).strip()
    )
    time_group.add_argument('start', type=otp.datetime,
                            help='Start time of the query or the date to query (if end is not specified).')
    time_group.add_argument('end', type=otp.datetime, nargs='?',
                            help='End time of the query.')
    time_group.add_argument('--tz', help='Time zone of the query. Default is local time zone.')

    rows_group = parser.add_argument_group('Rows')
    rows_group.add_argument('--limit', help='Number of rows to return. Default is all.', type=int)

    columns_group = parser.add_argument_group('Columns')
    columns_group = columns_group.add_mutually_exclusive_group()
    columns_group.add_argument('--columns', nargs='+',
                               metavar='COLUMN',
                               help='Names of columns to return. Default is all.')
    columns_group.add_argument('--columns-regex',
                               help='Regular expression for columns to return. Default is all.')

    output_group = parser.add_argument_group(
        'Output',
        description=dedent("""
            Types of output <format>:
                compact (default) - pandas output that fits to the screen, may be truncated;
                pandas            - pandas full output;
                count             - print the number of ticks;
                csv               - CSV, comma-separated;
                markdown          - Markdown format;
                json              - Json format {"columns":[],"data":[[...],[...],...]}.
        """).strip()
    )
    output_group.add_argument(
        '-o', '--output',
        default='compact',
        choices=['compact', 'pandas', 'count', 'csv', 'markdown', 'json'],
        metavar='<format>',
    )

    conf_group = parser.add_argument_group('OneTick configuration')
    conf_group.add_argument('--remote-ts', help='Address of the remote OneTick server to connect to.')

    prop_group = parser.add_argument_group(
        'OneTick Query Properties',
        description=dedent("""
            The values are not set by default. By default the settings set by onetick-py or OneTick will be used.
        """).strip()
    )
    prop_group.add_argument('--ignore-realtime-db',
                            action=argparse.BooleanOptionalAction, default=None,
                            help='Setting IGNORE_REALTIME_DB query property.')
    prop_group.add_argument('--ignore-ticks-in-unentitled-time-range',
                            action=argparse.BooleanOptionalAction, default=None,
                            help='Setting IGNORE_TICKS_IN_UNENTITLED_TIME_RANGE query property.')

    parser.add_argument('-v', '--verbose', action='count', default=0,
                        help='Print errors (-v) and warnings (-vv) and debug messages (-vvv).')

    return parser


def run(args: argparse.Namespace):
    try:
        __run_impl(args)
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)


def __run_impl(args: argparse.Namespace):

    otq.API_CONFIG['SHOW_STACK_WARNING'] = 0
    # disable running all additional queries with compatibility checks
    otp.config.disable_compatibility_checks = True

    if args.verbose >= 3:
        print(args)

    session = None

    path_db = Path(args.db)
    if path_db.is_dir() and os.sep in str(path_db):
        if args.remote_ts:
            raise ValueError(
                f"Parameter --remote-ts can't be used together with the database set to directory {args.db}."
            )

        session = otp.Session()
        db = otp.DB(
            path_db.name,
            db_properties={
                'day_boundary_tz': args.tz or otp.config.get('tz') or 'GMT',
            },
            db_locations=[{
                'location': path_db.absolute(),
                'start_time': otp.dt(1970, 1, 1),
                'end_time': otp.dt(2099, 12, 31),
            }]
        )
        session.use(db)
        args.db = path_db.name

    if args.remote_ts:
        session = otp.Session(
            otp.Config(
                locator=otp.RemoteTS(args.remote_ts)
            )
        )

    data = otp.DataSource(db=args.db, tick_type=args.tt, symbols=args.symbol, schema_policy='manual')

    if args.columns:
        fields = ','.join(args.columns)
        data.sink(otq.Passthrough(fields=fields, throw_for_missing_fields=False))
    elif args.columns_regex:
        data.sink(otq.Passthrough(fields=args.columns_regex, throw_for_missing_fields=False, use_regex=True))

    if args.limit is not None:
        if args.limit < 0:
            raise ValueError(f"Parameter --limit can't be negative {args.limit}")
        try:
            data = data.limit(args.limit)
        except RuntimeError:
            data = data[:args.limit]

    if args.output == 'count':
        data = otp.agg.count().apply(data)

    if args.end is None:
        run_kwargs = {'date': args.start}
    else:
        run_kwargs = {'start': args.start, 'end': args.end}

    query_properties = {}
    if args.ignore_realtime_db is not None:
        query_properties['IGNORE_REALTIME_DB'] = args.ignore_realtime_db
    if args.ignore_ticks_in_unentitled_time_range is not None:
        query_properties['IGNORE_TICKS_IN_UNENTITLED_TIME_RANGE'] = args.ignore_ticks_in_unentitled_time_range

    try:
        df: pd.DataFrame = otp.run(data,
                                   **run_kwargs,
                                   timezone=args.tz,
                                   query_properties=query_properties,
                                   print_symbol_errors=args.verbose > 0)
    finally:
        if session:
            session.close()

    if df.empty:
        return

    if args.output == 'count':
        print(df['VALUE'][0])
    elif args.output == 'compact':
        print(df)
    elif args.output == 'pandas':
        print(df.to_string(index=False))
    elif args.output == 'csv':
        print(df.to_csv(index=False))
    elif args.output == 'markdown':
        print(df.to_markdown(index=False, tablefmt='github'))
    elif args.output == 'json':
        print(df.to_json(index=False, orient='split'))
    else:
        raise ValueError(f"Unsupported output type: '{args.output}'")


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    parser_impl(arg_parser)
    run(arg_parser.parse_args())
