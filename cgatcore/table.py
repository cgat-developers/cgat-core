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


def main(argv=None):
    if argv is None:
        argv = sys.argv

    parser = E.ArgumentParser()

    parser.add_argument("--version", action='version',
                        version='%(prog)s {version}'.format(version="1.0"))
    parser.add_argument(
        "-m", "--method", dest="methods", type=str, action="append",
        choices=("transpose", "normalize-by-max", "normalize-by-value",
                 "multiply-by-value", "percentile", "remove-header",
                 "normalize-by-table", "upper-bound", "lower-bound",
                 "kullback-leibler", "expand", "compress", "fdr", "grep",
                 "randomize-rows"),
        help="actions to perform on table.")
    parser.add_argument("-s", "--scale", dest="scale", type=float,
                        help="factor to scale matrix by.")
    parser.add_argument("-f", "--format", dest="format", type=str,
                        help="output number format")
    parser.add_argument("-p", "--parameters", dest="parameters", type=str,
                        help="Parameters for various functions.")
    parser.add_argument(
        "--transpose", dest="transpose", action="store_true",
        help="transpose table.")
    parser.set_defaults(
        methods=[], scale=1.0, has_headers=True, format=None,
        value=0.0, parameters="", columns="all", transpose=False)

    (args, unknown) = E.start(parser, unknowns=True)
    args.parameters = args.parameters.split(",")

    if args.group_column:
        args.group = True
        args.group_column -= 1

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

    elif args.as_column:
        fields, table = CSV.readTable(
            args.stdin, with_header=args.has_headers, as_rows=True)
        args.columns = get_columns(fields, args.columns)
        table = list(zip(*table))
        args.stdout.write("value\n")
        for column in args.columns:
            args.stdout.write("\n".join(table[column]) + "\n")

    elif args.group:
        read_and_group_table(args.stdin, args)

    else:
        fields, table = CSV.readTable(
            args.stdin, with_header=args.has_headers, as_rows=False)
        table = [list(x) for x in table]

        ncols = len(fields)
        if len(table) == 0:
            raise ValueError("table is empty")

        nrows = len(table[0])
        E.info("processing table with %i rows and %i columns" % (nrows, ncols))
        args.columns = get_columns(fields, args.columns)

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

            elif method in ("lower-bound", "upper-bound"):
                boundary = float(args.parameters[0])
                del args.parameters[0]

                new_value = float(args.parameters[0])
                del args.parameters[0]

                if method == "upper-bound":
                    for c in args.columns:
                        for r in range(nrows):
                            if isinstance(table[c][r], float) and table[c][r] > boundary:
                                table[c][r] = new_value
                else:
                    for c in args.columns:
                        for r in range(nrows):
                            if isinstance(table[c][r], float) and table[c][r] < boundary:
                                table[c][r] = new_value

            elif method == "kullback-leibler":
                args.stdout.write("category1\tcategory2\tkl1\tkl2\tmean\n")
                format = args.format or "%f"
                for x in range(0, len(args.columns) - 1):
                    for y in range(x + 1, len(args.columns)):
                        c1, c2 = args.columns[x], args.columns[y]
                        e1, e2 = 0, 0
                        for z in range(nrows):
                            p, q = table[c1][z], table[c2][z]
                            e1 += p * math.log(p / q)
                            e2 += q * math.log(q / p)
                        args.stdout.write("%s\t%s\t%s\t%s\t%s\n" % (
                            fields[c1], fields[c2], format % e1, format % e2, format % ((e1 + e2) / 2)))
                E.stop()
                sys.exit(0)

            elif method == "fdr":
                pvalues = [table[c][r] for c in args.columns for r in range(nrows)]
                assert max(pvalues) <= 1.0, f"pvalues > 1 in table: max={max(pvalues)}"
                assert min(pvalues) >= 0, f"pvalue < 0 in table: min={min(pvalues)}"
                qvalues = list(map(str, Stats.adjustPValues(pvalues, method=args.fdr_method)))

                if args.fdr_add_column is None:
                    x = 0
                    for c in args.columns:
                        table[c] = qvalues[x:x + nrows]
                        x += nrows
                else:
                    fields += [args.fdr_add_column + fields[c] for c in args.columns]
                    x = 0
                    for c in args.columns:
                        table.append(qvalues[x:x + nrows])
                        x += nrows
                    ncols += len(args.columns)

        if args.format is not None:
            for c in args.columns:
                for r in range(nrows):
                    if isinstance(table[c][r], float):
                        table[c][r] = args.format % table[c][r]

        args.stdout.write("\t".join(fields) + "\n")
        for r in range(nrows):
            args.stdout.write("\t".join(map(str, [table[c][r] for c in range(ncols)])) + "\n")

    E.stop()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
