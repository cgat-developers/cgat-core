'''
CSV2DB.py - utilities for uploading a table to database
=======================================================

:Tags: Python

Purpose
-------

create a table from a csv separated file and load data into it.

This module supports backends for postgres and sqlite3. Column types are
auto-detected.

.. todo::

   Use file import where appropriate to speed up loading. Currently, this is
   not always the case.

Usage
-----

Documentation
-------------

Code
----

'''

import sys
import pandas
import re

from cgatcore import experiment as E
from cgatcore import database as database


def quote_tablename(name, quote_char="_", flavour="sqlite"):

    if flavour == "sqlite":
        # no special characters. Column names can not start with a number.
        if name[0] in "0123456789":
            name = "_" + name
        return re.sub("[-(),\[\].:]", "_", name)
    elif flavour in ("mysql", "postgres"):
        if name[0] in "0123456789":
            name = "_" + name
        return re.sub("[-(),\[\]]:", "_", name)


def get_flavour(database_url):
    if "sqlite" in database_url:
        return "sqlite"
    elif "mysql" in database_url:
        return "mysql"
    elif "postgres" in database_url:
        return "postgres"
    else:
        return "sqlite"


def run(infile, options, chunk_size=10000):

    # for backwards compatibility
    if options.retry:
        options.retries = 20
    else:
        options.retries = -1

    flavour = get_flavour(options.database_url)

    tablename = quote_tablename(options.tablename,
                                flavour=flavour)

    dbhandle = database.connect(url=options.database_url)

    if "tab" in options.dialect:
        separator = "\t"
    else:
        separator = ","

    if options.append:
        if_exists = "append"
    else:
        if_exists = "replace"

    # handle header logic up-front
    if options.replace_header:
        if options.header_names is None:
            raise ValueError("No replacement headers provided")
        header = 0
        names = options.header_names
    else:
        if options.header_names is None:
            header = 0
            names = None
        else:
            header = None
            names = options.header_names

    counter = E.Counter()
    try:
        for idx, df in enumerate(pandas.read_csv(
                infile,
                header=header,
                names=names,
                sep=separator,
                index_col=False,
                comment="#",
                chunksize=options.chunk_size)):

            if idx == 0 and len(df) == 0:
                if not options.allow_empty:
                    raise ValueError("table is empty")

            if idx > 0:
                if_exists = "append"

            columns = list(df.columns)

            if options.lowercase_columns:
                columns = [x.lower() for x in columns]

            if options.first_column:
                columns[0] = options.first_column

            if options.ignore_columns:
                df = df[[x for x in df.columns if x not in options.ignore_columns]]

            if options.ignore_empty:
                empty_list = df.columns[df.isna().all()].tolist()
                if idx == 0:
                    empty_columns = set(empty_list)
                else:
                    empty_columns = empty_columns.intersection(empty_list)

            df.to_sql(tablename,
                      con=dbhandle,
                      schema=options.database_schema,
                      index=False,
                      if_exists=if_exists)

            counter.input += len(df)
    except pandas.errors.EmptyDataError:
        if not options.allow_empty:
            raise
        else:
            return

    nindex = 0
    for index in options.indices:
        nindex += 1
        try:
            statement = "CREATE INDEX %s_index%i ON %s (%s)" % (
                tablename, nindex, tablename, index)
            cc = database.executewait(dbhandle, statement, retries=options.retries)
            cc.close()
            E.info("added index on column %s" % (index))
            counter.indexes_created += 1
        except Exception as ex:
            E.info("adding index on column %s failed: %s" % (index, ex))

    if options.ignore_empty:
        counter.empty_columns = len(empty_columns)
        for column in empty_columns:
            try:
                statement = "ALTER TABLE %s DROP COLUMN %s".format(
                    tablename, column)
                cc = database.executewait(dbhandle, statement, retries=options.retries)
                cc.close()
                E.info("removed empty column %s" % (column))
                counter.empty_columns_removed += 1
            except Exception as ex:
                E.info("removing empty column {} failed".format(column))

    statement = "SELECT COUNT(*) FROM %s" % (tablename)
    cc = database.executewait(dbhandle, statement, retries=options.retries)
    result = cc.fetchone()
    cc.close()

    counter.output = result[0]

    E.info(counter)


def buildParser():

    parser = E.ArgumentParser(description=__doc__)

    parser.add_argument("--version", action='version', version="1.0")

    parser.add_argument("--csv-dialect", dest="dialect", type=str,
                        help="csv dialect to use.")

    parser.add_argument(
        "-m", "--map", dest="map", type=str, action="append",
        help="explicit mapping function for columns The format is "
        "column:type (e.g.: length:int).")

    parser.add_argument("-t", "--table", dest="tablename", type=str,
                        help="table name for all backends.")

    parser.add_argument(
        "-H", "--header-names", dest="header_names", type=str,
        help="',' separated list of column headers for files without "
        "column header")

    parser.add_argument("--replace-header", dest="replace_header",
                        action="store_true",
                        help="replace header with --header-names instead of "
                        "adding it.")

    parser.add_argument("-l", "--lowercase-fields", dest="lowercase_columns",
                        action="store_true",
                        help="force lower case column names "
                        )

    # parser.add_argument("-u", "--ignore-duplicates", dest="ignore_duplicates",
    #                   action="store_true",
    #                   help="ignore columns with duplicate names "
    #                   "[default=%default].")

    # parser.add_argument("-s", "--ignore-same", dest="ignore_same",
    #                   action="store_true",
    #                   help="ignore columns with identical values "
    #                   "[default=%default].")

    parser.add_argument("--chunk-size", dest="chunk_size", type=int,
                        help="chunk-size, upload table in block of rows "
                        )

    parser.add_argument("--ignore-column", dest="ignore_columns", type=str,
                        action="append",
                        help="ignore columns.")

    parser.add_argument("--rename-column", dest="rename_columns", type=str,
                        action="append",
                        help="rename columns.")

    parser.add_argument("--first-column", dest="first_column", type=str,
                        help="name of first column - permits loading CSV "
                        "table where the first "
                        "column name is the empty string.")

    parser.add_argument("-e", "--ignore-empty", dest="ignore_empty",
                        action="store_true",
                        help="ignore columns which are all empty ")

    # parser.add_argument("-q", "--quick", dest="insert_quick",
    #                   action="store_true",
    #                   help="try quick file based import - needs to "
    #                   "be supported by the backend [default=%default].")

    parser.add_argument("-i", "--add-index", dest="indices", type=str,
                        action="append",
                        help="create an index for the named column "
                        )

    parser.add_argument("-a", "--allow-empty-file", dest="allow_empty",
                        action="store_true",
                        help="allow empty table.")

    parser.add_argument("--retry", dest="retry", action="store_true",
                        help="retry if an SQL statement fails - warning: "
                        "THIS MIGHT CAUSE DEADLOCKS.")

    # parser.add_argument("-z", "--from-zipped", dest="from_zipped",
    #                   action="store_true",
    #                   help="input is zipped.")

    parser.add_argument("--append", dest="append",
                        action="store_true",
                        help="append to existing table.")

    parser.add_argument(
        "--utf8", dest="utf", action="store_true",
        help="standard in is encoded as UTF8 rather than local default"
        ", WARNING: does not strip comment lines yet")

    parser.set_defaults(
        map=[],
        dialect="excel-tab",
        lowercase_columns=False,
        tablename="csv",
        from_zipped=False,
        ignore_duplicates=False,
        ignore_identical=False,
        ignore_empty=False,
        insert_many=False,
        ignore_columns=[],
        rename_columns=[],
        header_names=None,
        replace_header=False,
        chunk_size=100000,
        backend="sqlite",
        indices=[],
        missing_values=("na", "NA", ),
        insert_quick=False,
        allow_empty=False,
        retry=False,
        utf=False,
        append=False,
    )

    return parser


def main(argv=sys.argv):

    parser = buildParser()

    (args) = E.start(parser, argv=argv,
                     add_database_options=True)

    if args.from_zipped:
        import gzip
        infile = gzip.GzipFile(fileobj=args.stdin, mode='r')

    else:
        infile = args.stdin

    if args.header_names:
        if "," in args.header_names:
            # sqlalchemy.exc.ArgumentError:
            #     Column must be constructed with a non-blank
            #     name or assign a non-blank .name before adding to a Table.
            replace_empty_strings = (lambda arg: '-' if len(arg) == 0 else arg)
            args.header_names = \
                [x for x in map(replace_empty_strings, args.header_names.split(','))]
        else:
            args.header_names = re.split("\s+", args.header_names.strip())

    run(infile, args)

    E.stop()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
