import sys
import math
import itertools
import collections
import random
from functools import reduce
import numpy

import cgatcore.experiment as E
import cgatcore.csvutils as CSV
import cgatcore.iotools as iotools


# The status of this module is unresolved. Functionality implemented
# here is used in the database.py module to massage tables before
# uploading.  On the other hand, some functionality relies on the
# cgat-apps/CGAT/Stats.py module.


def get_columns(fields, columns="all"):
    '''return columns to take.'''
    if columns == "all":
        return list(range(len(fields)))
    elif columns == "all-but-first":
        return list(range(1, len(fields)))
    else:
        map_field2column = dict([(y, x) for x, y in enumerate(fields)])
        c = []
        for x in columns.split(","):
            if x in map_field2column:
                c.append(map_field2column[x])
            else:
                c.append(int(x) - 1)
        return c


def read_and_transpose_table(infile, args):
    """read table from infile and transpose
    """
    rows = []
    if args.transpose_format == "default":

        for line in infile:
            if line[0] == "#":
                continue
            rows.append(line[:-1].split("\t"))

    elif args.transpose_format == "separated":
        for line in infile:
            if line[0] == "#":
                continue
            key, vals = line[:-1].split("\t")
            row = [key] + vals.split(args.separator)
            rows.append(row)

    ncols = max([len(x) for x in rows])
    nrows = len(rows)

    new_rows = [[""] * nrows for x in range(ncols)]

    for r in range(0, len(rows)):
        for c in range(0, len(rows[r])):
            new_rows[c][r] = rows[r][c]

    if args.set_transpose_field:
        new_rows[0][0] = args.set_transpose_field

    for row in new_rows:
        args.stdout.write("\t".join(row) + "\n")


def read_and_group_table(infile, args):
    """read table from infile and group.
    """
    fields, table = CSV.readTable(
        infile, with_header=args.has_headers, as_rows=True)
    args.columns = get_columns(fields, args.columns)
    assert args.group_column not in args.columns

    converter = float
    new_fields = [fields[args.group_column]] + [fields[x] for x in args.columns]

    if args.group_function == "min":
        f = min
    elif args.group_function == "max":
        f = max
    elif args.group_function == "sum":
        def f(z): return reduce(lambda x, y: x + y, z)
    elif args.group_function == "mean":
        f = numpy.mean
    elif args.group_function == "cat":
        def f(x): return ";".join([y for y in x if y != ""])
        converter = str
    elif args.group_function == "uniq":
        def f(x): return ";".join([y for y in set(x) if y != ""])
        converter = str
    elif args.group_function == "stats":
        # Stats lives in cgat-apps/CGAT
        def f(x): return str(Stats.DistributionalParameters(x))
        # update headers
        new_fields = [fields[args.group_column]]
        for c in args.columns:
            new_fields += list(["%s_%s" %
                                (fields[c], x) for x in Stats.DistributionalParameters().getHeaders()])

    # convert values to floats (except for group_column)
    # Delete rows with unconvertable values and not in args.columns
    new_table = []
    for row in table:
        skip = False
        new_row = [row[args.group_column]]

        for c in args.columns:
            if row[c] == args.missing_value:
                new_row.append(row[c])
            else:
                try:
                    new_row.append(converter(row[c]))
                except ValueError:
                    skip = True
                    break
        if not skip:
            new_table.append(new_row)
    table = new_table

    new_rows = CSV.groupTable(table,
                              group_column=0,
                              group_function=f)

    args.stdout.write("\t".join(new_fields) + "\n")
    for row in new_rows:
        args.stdout.write("\t".join(map(str, row)) + "\n")


def read_and_expand_table(infile, args):
    '''splits fields in table at separator.

    If a field in a row contains multiple values,
    the row is expanded into multiple rows such
    that all values have space.
    '''

    fields, table = CSV.readTable(
        infile, with_header=args.has_headers, as_rows=True)

    args.stdout.write("\t".join(fields) + "\n")

    for row in table:

        data = []
        for x in range(len(fields)):
            data.append(row[x].split(args.separator))

        nrows = max([len(d) for d in data])

        for d in data:
            d += [""] * (nrows - len(d))

        for n in range(nrows):
            args.stdout.write("\t".join([d[n] for d in data]) + "\n")


def read_and_collapse_table(infile, args, missing_value=""):
    '''collapse a table.

    Collapse a table of two columns with row names in the first
    column. Outputs a table with multiple columns for each row name.
    '''

    fields, table = CSV.readTable(
        infile, with_header=args.has_headers, as_rows=True)

    if len(fields) != 2:
        raise NotImplementedError("can only work on tables with two columns")

    values = collections.defaultdict(list)

    # column header after which to add
    separator = table[0][0]
    row_names = set([x[0] for x in table])

    row_name, value = table[0]

    values[row_name].append(value)
    added = set([row_name])
    for row_name, value in table[1:]:
        if row_name == separator:
            for r in row_names:
                if r not in added:
                    values[r].append(missing_value)
            added = set()

        values[row_name].append(value)
        added.add(row_name)

    for r in row_names:
        if r not in added:
            values[r].append(missing_value)

    sizes = set([len(x) for x in list(values.values())])
    assert len(sizes) == 1, "unequal number of row_names"
    size = list(sizes)[0]

    args.stdout.write(
        "row\t%s\n" % ("\t".join(["column_%i" % x for x in range(size)])))

    for key, row in list(values.items()):
        args.stdout.write("%s\t%s\n" % (key, "\t".join(row)))


def computeFDR(infile, args):
    '''compute FDR on a table.
    '''

    fields, table = CSV.readTable(
        infile, with_header=args.has_headers, as_rows=True)

    args.stdout.write("\t".join(fields) + "\n")

    for row in table:

        data = []
        for x in range(len(fields)):
            data.append(row[x].split(args.separator))

        nrows = max([len(d) for d in data])

        for d in data:
            d += [""] * (nrows - len(d))

        for n in range(nrows):
            args.stdout.write("\t".join([d[n] for d in data]) + "\n")


def read_and_join_table(infile, args):

    fields, table = CSV.readTable(
        infile, with_header=args.has_headers, as_rows=True)

    join_column = args.join_column - 1
    join_name = args.join_column_name - 1

    join_rows = list(set([x[join_column] for x in table]))
    join_rows.sort()

    join_names = list(set([x[join_name] for x in table]))
    join_names.sort()

    join_columns = list(
        set(range(len(fields))).difference(set((join_column, join_name))))
    join_columns.sort()

    new_table = []
    map_old2new = {}

    map_name2start = {}
    x = 1
    for name in join_names:
        map_name2start[name] = x
        x += len(join_columns)

    row_width = len(join_columns) * len(join_names)
    for x in join_rows:
        map_old2new[x] = len(map_old2new)
        new_row = [x, ] + ["na"] * row_width
        new_table.append(new_row)

    for row in table:
        row_index = map_old2new[row[join_column]]
        start = map_name2start[row[join_name]]
        for x in join_columns:
            new_table[row_index][start] = row[x]
            start += 1

    # print new table
    args.stdout.write(fields[join_column])
    for name in join_names:
        for column in join_columns:
            args.stdout.write(
                "\t%s%s%s" % (name, args.separator, fields[column]))
    args.stdout.write("\n")

    for row in new_table:
        args.stdout.write("\t".join(row) + "\n")


def read_and_randomize_rows(infile, args):
    """read table from stdin and randomize rows, keeping header."""

    c = E.Counter()
    if args.has_headers:
        keep_header = 1
    else:
        keep_header = 0
    for x in range(keep_header):
        c.header += 1
        args.stdout.write(infile.readline())

    lines = infile.readlines()
    c.lines_input = len(lines)
    random.shuffle(lines)
    args.stdout.write("".join(lines))
    c.lines_output = len(lines)
    E.info(c)


def main(argv=None):
    """script main.

    parses command line args in sys.argv, unless *argv* is given.
    """

    if argv is None:
        argv = sys.argv

    parser = E.ArgumentParser()

    parser.add_argument("--version", action='version',
                        version='%(prog)s {version}'.format(version="1.0"))

    parser.add_argument(
        "-m", "--method", dest="methods", type=str, action="append",
        choices=("transpose", "normalize-by-max", "normalize-by-value",
                 "multiply-by-value",
                 "percentile", "remove-header", "normalize-by-table",
                 "upper-bound", "lower-bound", "kullback-leibler",
                 "expand", "compress", "fdr", "grep",
                 "randomize-rows"),
        help="""actions to perform on table.""")

    parser.add_argument("-s", "--scale", dest="scale", type=float,
                        help="factor to scale matrix by.")

    parser.add_argument("-f", "--format", dest="format", type=str,
                        help="output number format")

    parser.add_argument("-p", "--parameters", dest="parameters", type=str,
                        help="Parameters for various functions.")

    parser.add_argument(
        "-t", "--header-names", dest="has_headers", action="store_true",
        help="matrix has row/column headers.")

    parser.add_argument("--transpose", dest="transpose", action="store_true",
                        help="transpose table.")

    parser.add_argument(
        "--set-transpose-field", dest="set_transpose_field", type=str,
        help="set first field (row 1 and col 1) to this value [%default].")

    parser.add_argument(
        "--transpose-format", dest="transpose_format", type=str,
        choices=("default", "separated", ),
        help="input format of un-transposed table")

    parser.add_argument(
        "--expand", dest="expand_table", action="store_true",
        help="expand table - multi-value cells with be expanded over "
        "several rows.")

    parser.add_argument("--no-headers", dest="has_headers", action="store_false",
                        help="matrix has no row/column headers.")

    parser.add_argument("--columns", dest="columns", type=str,
                        help="columns to use.")

    parser.add_argument("--file", dest="file", type=str,
                        help="columns to test from table.",
                        metavar="FILE")

    parser.add_argument("-d", "--delimiter", dest="delimiter", type=str,
                        help="delimiter of columns.",
                        metavar="DELIM")

    parser.add_argument(
        "-V", "--invert-match", dest="invert_match",
        action="store_true",
        help="invert match.")

    parser.add_argument("--sort-by-rows", dest="sort_rows", type=str,
                        help="output order for rows.")

    parser.add_argument("-a", "--value", dest="value", type=float,
                        help="value to use for various algorithms.")

    parser.add_argument(
        "--group", dest="group_column", type=int,
        help="group values by column. Supply an integer column ")

    parser.add_argument("--group-function", dest="group_function", type=str,
                        choices=(
                            "min", "max", "sum", "mean", "stats", "cat", "uniq"),
                        help="function to group values by.")

    parser.add_argument("--join-table", dest="join_column", type=int,
                        help="join rows in a table by columns.")

    parser.add_argument(
        "--collapse-table", dest="collapse_table", type=str,
        help="collapse a table. Value determines the missing variable ")

    parser.add_argument(
        "--join-column-name", dest="join_column_name", type=int,
        help="use this column as a prefix.")

    parser.add_argument(
        "--flatten-table", dest="flatten_table", action="store_true",
        help="flatten a table.")

    parser.add_argument("--as-column", dest="as_column", action="store_true",
                        help="output table as a single column.")

    parser.add_argument(
        "--split-fields", dest="split_fields", action="store_true",
        help="split fields.")

    parser.add_argument(
        "--separator", dest="separator", type=str,
        help="separator for multi-valued fields.")

    parser.add_argument(
        "--fdr-method", dest="fdr_method", type=str,
        choices=(
            "BH", "bonferroni", "holm", "hommel", "hochberg", "BY"),
        help="method to perform multiple testing correction by controlling "
        "the fdr.")

    parser.add_argument(
        "--fdr-add-column", dest="fdr_add_column", type=str,
        help="add new column instead of replacing existing columns. "
        "The value of the option will be used as prefix if there are "
        "multiple columns")

    # IMS: add option to use a column as the row id in flatten
    parser.add_argument(
        "--id-column", dest="id_column", type=str,
        help="list of column(s) to use as the row id when flattening "
        "the table. If None, then row number is used.")

    parser.add_argument(
        "--variable-name", dest="variable_name", type=str,
        help="the column header for the 'variable' column when flattening ")

    parser.add_argument(
        "--value-name", dest="value_name", type=str,
        help="the column header for the 'value' column when flattening ")

    parser.set_defaults(
        methods=[],
        scale=1.0,
        has_headers=True,
        format=None,
        value=0.0,
        parameters="",
        columns="all",
        transpose=False,
        set_transpose_field=None,
        transpose_format="default",
        group=False,
        group_column=0,
        group_function="mean",
        missing_value="na",
        sort_rows=None,
        flatten_table=False,
        collapse_table=None,
        separator=";",
        expand=False,
        join_column=None,
        join_column_name=None,
        compute_fdr=None,
        as_column=False,
        fdr_method="BH",
        fdr_add_column=None,
        id_column=None,
        variable_name="column",
        value_name="value",
        file=None,
        delimiter="\t",
        invert_match=False,
    )

    (args, unknown) = E.start(parser, unknowns=True)

    args.parameters = args.parameters.split(",")

    if args.group_column:
        args.group = True
        args.group_column -= 1

    ######################################################################
    ######################################################################
    ######################################################################
    # if only to remove header, do this quickly
    if args.methods == ["remove-header"]:

        first = True
        for line in args.stdin:
            if line[0] == "#":
                continue
            if first:
                first = False
                continue
            args.stdout.write(line)

    elif args.transpose or "transpose" in args.methods:

        read_and_transpose_table(args.stdin, args)

    elif args.flatten_table:
        # IMS: bug fixed to make work. Also added options for keying
        # on a particular and adding custom column headings

        fields, table = CSV.readTable(
            args.stdin, with_header=args.has_headers, as_rows=True)

        args.columns = get_columns(fields, args.columns)

        if args.id_column:
            id_columns = [int(x) - 1 for x in args.id_column.split(",")]
            id_header = "\t".join([fields[id_column]
                                   for id_column in id_columns])
            args.columns = [
                x for x in args.columns if x not in id_columns]
        else:
            id_header = "row"

        args.stdout.write(
            "%s\t%s\t%s\n" % (id_header, args.variable_name,
                              args.value_name))

        for x, row in enumerate(table):

            if args.id_column:
                row_id = "\t".join([row[int(x) - 1]
                                    for x in args.id_column.split(",")])
            else:
                row_id = str(x)

            for y in args.columns:
                args.stdout.write(
                    "%s\t%s\t%s\n" % (row_id, fields[y], row[y]))

    elif args.as_column:

        fields, table = CSV.readTable(
            args.stdin, with_header=args.has_headers, as_rows=True)
        args.columns = get_columns(fields, args.columns)
        table = list(zip(*table))

        args.stdout.write("value\n")

        for column in args.columns:
            args.stdout.write("\n".join(table[column]) + "\n")

    elif args.split_fields:

        # split comma separated fields
        fields, table = CSV.readTable(args.stdin,
                                      with_header=args.has_headers,
                                      as_rows=True)

        args.stdout.write("%s\n" % ("\t".join(fields)))

        for row in table:
            row = [x.split(args.separator) for x in row]
            for d in itertools.product(*row):
                args.stdout.write("%s\n" % "\t".join(d))

    elif args.group:
        read_and_group_table(args.stdin, args)

    elif args.join_column:
        read_and_join_table(args.stdin, args)

    elif args.expand_table:
        read_and_expand_table(args.stdin, args)

    elif args.collapse_table is not None:
        read_and_collapse_table(args.stdin, args, args.collapse_table)

    elif "randomize-rows" in args.methods:
        read_and_randomize_rows(args.stdin, args)

    elif "grep" in args.methods:

        args.columns = [int(x) - 1 for x in args.columns.split(",")]

        patterns = []

        if args.file:
            infile = iotools.open_file(args.file, "r")
            for line in infile:
                if line[0] == "#":
                    continue
                patterns.append(line[:-1].split(args.delimiter)[0])
        else:
            patterns = args

        for line in args.stdin:

            data = line[:-1].split(args.delimiter)
            found = False

            for c in args.columns:

                if data[c] in patterns:
                    found = True
                    break

            if (not found and args.invert_match) or (found and not args.invert_match):
                print(line[:-1])
    else:

        ######################################################################
        ######################################################################
        ######################################################################
        # Apply remainder of transformations
        fields, table = CSV.readTable(
            args.stdin, with_header=args.has_headers, as_rows=False)
        # convert columns to list
        table = [list(x) for x in table]

        ncols = len(fields)
        if len(table) == 0:
            raise ValueError("table is empty")

        nrows = len(table[0])

        E.info("processing table with %i rows and %i columns" % (nrows, ncols))

        args.columns = get_columns(fields, args.columns)

        # convert all values to float
        for c in args.columns:
            for r in range(nrows):
                try:
                    table[c][r] = float(table[c][r])
                except ValueError:
                    continue

        for method in args.methods:

            if method == "normalize-by-value":

                value = float(args.parameters[0])
                del args.parameters[0]

                for c in args.columns:
                    table[c] = [x / value for x in table[c]]

            elif method == "multiply-by-value":

                value = float(args.parameters[0])
                del args.parameters[0]

                for c in args.columns:
                    table[c] = [x * value for x in table[c]]

            elif method == "normalize-by-max":

                for c in args.columns:
                    m = max(table[c])
                    table[c] = [x / m for x in table[c]]

            elif method == "kullback-leibler":
                args.stdout.write("category1\tcategory2\tkl1\tkl2\tmean\n")
                format = args.format
                if format is None:
                    format = "%f"

                for x in range(0, len(args.columns) - 1):
                    for y in range(x + 1, len(args.columns)):
                        c1 = args.columns[x]
                        c2 = args.columns[y]
                        e1 = 0
                        e2 = 0
                        for z in range(nrows):
                            p = table[c1][z]
                            q = table[c2][z]
                            e1 += p * math.log(p / q)
                            e2 += q * math.log(q / p)

                        args.stdout.write("%s\t%s\t%s\t%s\t%s\n" % (
                            fields[c1], fields[c2],
                            format % e1,
                            format % e2,
                            format % ((e1 + e2) / 2)))
                E.stop()
                sys.exit(0)

            elif method == "rank":

                for c in args.columns:
                    tt = table[c]
                    t = list(zip(tt, list(range(nrows))))
                    t.sort()
                    for i, n in zip([x[1] for x in t], list(range(nrows))):
                        tt[i] = n

            elif method in ("lower-bound", "upper-bound"):

                boundary = float(args.parameters[0])
                del args.parameters[0]
                new_value = float(args.parameters[0])
                del args.parameters[0]

                if method == "upper-bound":
                    for c in args.columns:
                        for r in range(nrows):
                            if isinstance(table[c][r], float) and \
                                    table[c][r] > boundary:
                                table[c][r] = new_value
                else:
                    for c in args.columns:
                        for r in range(nrows):
                            if isinstance(table[c][r], float) and \
                                    table[c][r] < boundary:
                                table[c][r] = new_value

            elif method == "fdr":
                pvalues = []
                for c in args.columns:
                    pvalues.extend(table[c])

                assert max(pvalues) <= 1.0, "pvalues > 1 in table: max=%s" % \
                    str(max(pvalues))
                assert min(pvalues) >= 0, "pvalue < 0 in table: min=%s" % \
                    str(min(pvalues))

                # convert to str to avoid test for float downstream
                qvalues = list(map(
                    str, Stats.adjustPValues(pvalues,
                                             method=args.fdr_method)))

                if args.fdr_add_column is None:
                    x = 0
                    for c in args.columns:
                        table[c] = qvalues[x:x + nrows]
                        x += nrows
                else:
                    # add new column headers
                    if len(args.columns) == 1:
                        fields.append(args.fdr_add_column)
                    else:
                        for co in args.columns:
                            fields.append(args.fdr_add_column + fields[c])

                    x = 0
                    for c in args.columns:
                        # add a new column
                        table.append(qvalues[x:x + nrows])
                        x += nrows
                    ncols += len(args.columns)

            elif method == "normalize-by-table":

                other_table_name = args.parameters[0]
                del args.parameters[0]
                other_fields, other_table = CSV.readTable(
                    iotools.open_file(other_table_name, "r"),
                    with_header=args.has_headers,
                    as_rows=False)

                # convert all values to float
                for c in args.columns:
                    for r in range(nrows):
                        try:
                            other_table[c][r] = float(other_table[c][r])
                        except ValueError:
                            continue

                # set 0s to 1 in the other matrix
                for c in args.columns:
                    for r in range(nrows):
                        if isinstance(table[c][r], float) and \
                                isinstance(other_table[c][r], float) and \
                                other_table[c][r] != 0:
                            table[c][r] /= other_table[c][r]
                        else:
                            table[c][r] = args.missing_value

        # convert back
        if args.format is not None:
            for c in args.columns:
                for r in range(nrows):
                    if isinstance(table[c][r], float):
                        table[c][r] = format % table[c][r]

        args.stdout.write("\t".join(fields) + "\n")
        if args.sort_rows:
            old2new = {}
            for r in range(nrows):
                old2new[table[0][r]] = r
            for x in args.sort_rows.split(","):
                if x not in old2new:
                    continue
                r = old2new[x]
                args.stdout.write(
                    "\t".join(map(str,
                                  [table[c][r] for c in range(ncols)])) + "\n")
        else:
            for r in range(nrows):
                args.stdout.write(
                    "\t".join(map(str,
                                  [table[c][r] for c in range(ncols)])) + "\n")

    E.stop()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
