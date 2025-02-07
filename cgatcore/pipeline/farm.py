# !/bin/env python
r'''farm.py - process data stream on cluster
===========================================


Purpose
-------

This script reads data from stdin and splits it into independent
chunks to be executed on a cluster.

Usage
-----

As a basic, but not very useful example, the following command will
take the input of the file ``go.txt``, split the file by the contents
of the first column and execute a perl command on them::

   cat go.txt | farm.py --split-at-colum=1 perl -p -e "s/GO/gaga/"

Type::

   python farm.py --help

for command line help.

Documentation
-------------

The input on stdin is split for embarrasingly parallel jobs.  The
``--split-at`` options describe how standard input is to be split. A
temporary directory is created in the current directory. This
directory has be visible on the cluster nodes and accessible under the
same name.

The output is written to stdout. Results are returned in the same
order as they are submitted. The script implements a few generic ways
to combine tabular output, for example to avoid duplicating header
lines. The script is also able to handle multiple outputs for jobs.

On error, error messages are echoed and nothing is returned.  The
temporary directory is not deleted to allow manual recovery.

Examples
--------

The following command will split the file "go" at the first column and
execute the command perl -p -e "s/GO/gaga/"::

   cat go | farm.py --split-at-colum=1 perl -p -e "s/GO/gaga/"

The following command will split a fasta file at each entry and
compute an approximate sequence length::

   cat genome.fasta | farm.py --split-at-regex="^>(\S+)" "wc -c"

The following command will split a fasta file at every 10 sequences::

   cat genome.fasta | farm.py --split-at-regex="^>(\S+)" --chunk-size=10 "wc -c"

.. todo::

   implement continuation of jobs
   implement better error messages
   use sge array jobs for job control

Command line options
--------------------

'''

import os
import sys
import re
import glob
import subprocess
import tempfile
import shutil

from multiprocessing.pool import Pool, ThreadPool

import cgatcore.experiment as E
import cgatcore.iotools as iotools

import cgatcore.pipeline as P


def chunk_iterator_lines(infile, args, prefix, use_header=False):
    """split by lines."""

    chunk_size = args[0]
    n = 0
    filename = "%s/%010i.in" % (prefix, n)
    outfile = iotools.open_file(filename, "w")
    header = None

    for line in infile:
        if line[0] == "#":
            continue

        if not header and n == 0 and use_header:
            header = line
            outfile.write(header)
            continue

        n += 1

        if n % chunk_size == 0:
            outfile.close()
            yield filename
            filename = "%s/%010i.in" % (prefix, n)
            outfile = iotools.open_file(filename, "w")
            if header:
                outfile.write(header)

        outfile.write(line)
    outfile.close()
    yield filename


def chunk_iterator_column(infile, args, prefix, use_header=False):
    """split at column.

    The table need not be sorted by this column.
    If num_files is given, files will randomly created
    and tags according to column randomly assigned.


    """

    column, max_files = args
    files = iotools.FilePool()
    header = False

    if max_files:
        map_tag2file = {}

    for line in infile:
        if line[0] == "#":
            continue

        if not header and use_header:
            files.setHeader(line)
            header = True
            continue

        key = line[:-1].split("\t")[column]
        if max_files:
            if key in map_tag2file:
                key = map_tag2file[key]
            else:
                n = "%010i" % (len(map_tag2file) % max_files)
                map_tag2file[key] = n
                key = n

        files.write("%s/%s.in" % (prefix, key), line)

    for filename, count in list(files.items()):
        E.info("created file %s with %i items" % (filename, count))
        yield filename


def chunk_iterator_regex_group(infile, args, prefix, use_header=False):
    """group by regular expression is true.

    Entries need to be consecutive.
    """

    rex = args[0]
    column = args[1]
    chunk_size = args[2]
    last = None
    header = None
    n = chunk_size
    outfile = None
    filename = None

    for line in infile:

        if line[0] == "#":
            continue

        if not header and use_header:
            header = line
            continue

        try:
            this = rex.search(line[:-1]).groups()[0]
        except IndexError:
            if outfile:
                outfile.write(line)
            continue
        except AttributeError:
            if outfile:
                outfile.write(line)
            continue

        if last != this and n >= chunk_size:
            if last:
                outfile.close()
                yield filename

            last = this

            filename = "%s/%s.in" % (prefix, this)
            outfile = iotools.open_file(filename, "w")
            if header:
                outfile.write(header)
            n = 0

        outfile.write(line)
        n += 1

    if outfile:
        outfile.close()
        yield filename


def chunk_iterator_regex_split(infile, args, prefix, use_header=False):
    """split where regular expression is true.
    """

    rex = args[0]
    chunk_size = args[2]
    max_lines = args[3]

    nlines = 0
    n = 0
    filename = "%s/%010i.in" % (prefix, n)
    outfile = iotools.open_file(filename, "w")

    for line in infile:

        if line[0] == "#":
            continue
        if rex.search(line[:-1]):
            if n > 0 and (n % chunk_size == 0 or (max_lines and nlines > max_lines)):
                outfile.close()
                yield filename
                filename = "%s/%010i.in" % (prefix, n)
                outfile = iotools.open_file(filename, "w")
                nlines = 0

            n += 1

        outfile.write(line)
        nlines += 1

    outfile.close()
    yield filename


class MapperGlobal:

    def __init__(self, pattern="%06i"):
        self.mMap = {}
        assert "%" in pattern, "please supply a pattern"
        self.mPattern = pattern

    def __call__(self, fn, id):
        if id not in self.mMap:
            self.mMap[id] = self.mPattern % (len(self.mMap) + 1)
        return self.mMap[id]


class MapperLocal:

    def __init__(self, pattern="%06i"):
        self.mMap = {}
        assert "%" in pattern, "please supply a pattern"
        self.mPattern = pattern

    def __call__(self, fn, id):
        key = "%s-%s" % (fn, id)
        if key not in self.mMap:
            self.mMap[key] = self.mPattern % (len(self.mMap) + 1)
        return self.mMap[key]


class MapperEmpty:

    def __init__(self):
        pass

    def __call__(self, fn, id):
        return id


class ResultBuilder:

    """the default result builder for table formatted output.

    field_index :
    """

    def __init__(self,
                 mapper=None,
                 field_index=None,
                 field_name=None,
                 header_regex=None):
        self.mMapper = mapper
        self.mFieldIndex = field_index
        self.mFieldName = field_name
        self.header = None
        self.nfields = None
        self.header_regex = header_regex

    def parseHeader(self, infile, outfile, options):
        """parse header in infile."""
        # skip comments until header
        while 1:
            line = infile.readline()
            if not line:
                break
            if self.header_regex:
                if self.header_regex.search(l):
                    break
            elif line[0] != "#":
                break
            options.stdlog.write(l)

        # print only the first header and check if
        # all the headers are the same.
        if self.header:
            if self.header != line:
                raise ValueError(
                    "inconsistent header in file %s\n"
                    "got=%s\nexpected=%s" % (infile, line, self.header))
        else:
            outfile.write(line)
            self.header = line
            self.nfields = line.count("\t")
            if self.nfields == 0:
                E.warn("only single column in header: %s" % line[:-1])

            if self.mFieldIndex is None and self.mFieldName:
                try:
                    self.mFieldIndex = self.header.split(
                        "\t").index(self.mFieldName)
                except ValueError:
                    E.warn("no mapping, can not find field %s in %s" %
                           (self.mFieldName, self.header))
                    self.mFieldName = None

                E.debug(
                    "substituting field: %s, %s" %
                    (self.mFieldName, self.mFieldIndex))

    def __call__(self, filenames, outfile, options):

        for fi, fn in filenames:
            E.debug("# merging %s" % fn)
            infile = iotools.open_file(fn, "r")

            if options.output_header:
                self.parseHeader(infile, outfile, options)

            for line in infile:
                nfields = l.count("\t")

                if line[0] == "#":
                    options.stdlog.write(l)
                elif self.nfields is not None and nfields != self.nfields:
                    # validate number of fields in row, raise warning
                    # for those not matching and skip.
                    E.warn(
                        "# line %s has unexpected number of fields: %i != %i" %
                        (line[:-1], nfields, self.nfields))
                else:
                    if self.mFieldIndex is not None:
                        data = line[:-1].split("\t")
                        try:
                            data[self.mFieldIndex] = self.mMapper(
                                fi, data[self.mFieldIndex])
                        except IndexError:
                            raise IndexError(
                                "can not find field %i in %s" %
                                (self.mFieldIndex, line))
                        line = "\t".join(data) + "\n"

                    outfile.write(l)
            infile.close()


class ResultBuilderFasta(ResultBuilder):

    def __init__(self, *args, **kwargs):
        ResultBuilder.__init__(self, *args, **kwargs)

    def __call__(self, filenames, outfile, options):
        for fi, fn in filenames:
            infile = iotools.open_file(fn, "r")
            for line in infile:
                if line[0] == "#":
                    options.stdlog.write(line)
                    continue
                elif line[0] == ">":
                    x = re.search(r">(\S+)", line[:-1])
                    id = self.mMapper(fi, x.groups()[0])
                    line = ">%s%s" % (id, line[x.end(0):])
                outfile.write(line)
            infile.close()


class ResultBuilderBinary(ResultBuilder):

    '''simply concatenate output files (without any parsing).'''

    def __init__(self, *args, **kwargs):
        ResultBuilder.__init__(self, *args, **kwargs)

    def __call__(self, filenames, outfile, options):
        for fi, fn in filenames:
            shutil.copyfileobj(iotools.open_file(fn, "r"), outfile)


class ResultBuilderCopies(ResultBuilder):

    '''create indexed copiers.'''

    def __init__(self, *args, **kwargs):
        ResultBuilder.__init__(self, *args, **kwargs)

    def __call__(self, filenames, outfile, options):
        idx = 0
        base, ext = os.path.splitext(outfile.name)
        for fi, fn in filenames:
            idx += 1
            shutil.copyfile(fn, base + ".%i" % idx + ext)


class ResultBuilderLog(ResultBuilder):

    """processor for log files."""

    def __init__(self, *args, **kwargs):
        ResultBuilder.__init__(self, *args, **kwargs)

    def __call__(self, filenames, outfile, options):
        for fi, fn in filenames:
            infile = iotools.open_file(fn, "r")
            outfile.write(
                "######### logging output for %s ###################\n" % fi)
            for line in infile:
                outfile.write(line)
            infile.close()


def build_command(data):

    filename, cmd, options, tmpdir, subdirs = data

    if subdirs:
        outdir = "%s.dir/" % (filename)
        os.mkdir(outdir)
        cmd = re.sub("%DIR%", outdir, cmd)

    x = re.search(r"'--log=(\S+)'", cmd) or re.search(r"'--L\s+(\S+)'", cmd)
    if x:
        logfile = filename + ".log"
        cmd = cmd[:x.start()] + "--log=%s" % logfile + cmd[x.end():]
    else:
        logfile = filename + ".out"

    cmd = re.sub("%STDIN%", filename, cmd)

    cmd += " 2>> {filename}.err > {filename}.out".format(filename=filename)

    return cmd


def hasFinished(retcode, filename, output_tag, logfile):
    """check if a run has finished."""

    E.info("checking status of job %s with returncode %i" %
           (filename, retcode))
    if retcode != 0:
        try:
            if not output_tag or not re.search(output_tag,
                                               iotools.get_last_line(logfile)):
                return False
        except IOError:
            E.warn("could not read output_tag from files %s" % (logfile))
            return False
    return True


def get_option_parser():
    """create parser and add options."""

    parser = E.OptionParser(version="%prog version: $Id$",
                            usage=globals()["__doc__"])

    parser.add_option(
        "--split-at-lines", dest="split_at_lines", type="int",
        help="split jobs according to line number [%default].")

    parser.add_option(
        "--split-at-column", dest="split_at_column", type="int",
        help="split jobs according to column. Columns start at number 1 "
        "and the input should be sorted by this column [%default].")

    parser.add_option(
        "--group-by-regex", dest="group_by_regex", type="string",
        help="group jobs according to a regular expression [%default].")

    parser.add_option(
        "--split-at-regex", dest="split_at_regex", type="string",
        help="split jobs according to a regular expression [%default].")

    parser.add_option(
        "--split-at-tag", dest="split_at_tag", type="int",
        help="split a file at a tag [%default].")

    parser.add_option(
        "--chunk-size", dest="chunksize", type="int",
        help="when splitting at regex or tag, aggregate x entries [%default].")

    parser.add_option(
        "--debug", dest="debug", action="store_true",
        help="debug mode. Do not delete temporary file [%default].")

    parser.add_option(
        "--dry-run", dest="dry_run", action="store_true",
        help="dry run. Do not split input and simply forward stdin to stdout. "
        "Useful for debugging the command [%default].")

    parser.add_option(
        "--input-header", dest="input_header", action="store_true",
        help="The input stream contains a table header. "
        "This header is replicated for each job [%default].")

    parser.add_option(
        "--output-header", dest="output_header", action="store_true",
        help="The output jobs contain a table header. "
        "The header is removed for each job except for the first [%default].")

    parser.add_option(
        "--output-regex-header", dest="output_regex_header", type="string",
        help="Regular expression for header (in stdout stream). Any lines "
        "before the first line matching this regular expression are ignored"
        "[%default].")

    parser.add_option(
        "--output-tag", dest="output_tag", type="string",
        help="The output jobs contain a tag in the last line denoting "
        "job completion. If the unix return value denotes an error, the "
        "presence of this tag is checked [%default].")

    parser.add_option(
        "--subdirs", dest="subdirs", action="store_true",
        help="Run within separate subdirs for jobs. This permits "
        "multiple output streams. Use a placeholder %DIR% if you supply "
        "the ouput pattern as a command line option [%default].")

    parser.add_option(
        "-T", "--temp-dir", dest="tmpdir", type="string",
        help="Temporary directory to be used. Default is the current "
        "directory [%default].")

    parser.add_option("--max-files", dest="max_files", type="int",
                      help="create at most x files [%default].")

    parser.add_option(
        "--max-lines", dest="max_lines", type="int",
        help="in addition to splitting into chunksize, also split if "
        "more than max-lines is reached [%default].")

    parser.add_option(
        "--renumber", dest="renumber", type="string",
        help="renumber ids consecutively, supply a pattern [%default].")

    parser.add_option(
        "--renumber-column", dest="renumber_column", type="string",
        action="append",
        help="specify column to renumber. The format is regex:column, "
        "for example csv:1 or csv:id [%default].")

    parser.add_option(
        "-r", "--reduce", dest="reduce", type="string", action="append",
        help="Add reduce functions for specific files. The format is "
        "file:reducer. The default reducer is 'table' for all files "
        "[%default].")

    parser.add_option(
        "-m", "--map", dest="map", type="string", action="append",
        help="Map specific columns in tables. The format is "
        "file:column:pattern, for example .table:1:%06i [%default].")

    parser.add_option(
        "--resume", dest="resume", type="string",
        help="resume aborted run from files in dir [%default]")

    parser.add_option(
        "--collect", dest="collect", type="string",
        help="collect files in dir and process as normally "
        "[%default]")

    parser.add_option(
        "--is-binary", dest="binary", action="store_true",
        help="the output is binary - files are concatenated "
        "without parsing [%default]")

    parser.add_option(
        "--resubmit", dest="resubmit", type="int",
        help="if a job fails, automatically resubmit # times. Set to 0 "
        "in order to disable resubmission [%default]")

    parser.add_option(
        "--fail", dest="resubmit", action="store_false",
        help="if a job fails, do not resubmit [%default]")

    parser.add_option(
        "--bashrc", dest="bashrc", type="string",
        help="bashrc file to use [%default]")

    parser.add_option(
        "--method", dest="method", type="choice",
        choices=("multiprocessing", "threads", "drmaa"),
        help="method to submit jobs [%default]")

    parser.add_option(
        "--job-memory", dest="job_memory", type="string",
        help="per-job memory requirement."
        "Unit must be specified, eg. 100M, 1G ")

    parser.add_option(
        "-e", "--env", dest="environment", type="string", action="append",
        help="environment variables to be passed to the jobs [%default]")

    parser.add_option(
        "--output-filename-pattern", dest="output_pattern", type="string",
        help="Pattern for secondary output filenames. Should contain a '%s' "
        "[%default].")

    parser.set_defaults(
        split_at_lines=None,
        split_at_column=None,
        split_at_regex=None,
        group_by_regex=None,
        split_at_tag=None,
        chunksize=100,
        cluster_cmd='qrsh -cwd -now n',
        bashrc="~/.bashrc",
        input_header=False,
        output_header=False,
        output_regex_header=None,
        debug=False,
        dry_run=False,
        tmpdir="./",
        subdirs=False,
        renumber=None,
        output_tag="# job finished",
        map=[],
        reduce=[],
        resume=None,
        renumber_column=[],
        resubmit=5,
        collect=None,
        method="drmaa",
        job_memory=None,
        max_files=None,
        max_lines=None,
        binary=False,
        environment=[],
        output_pattern="%s",
    )

    # stop parsing options at the first argument
    parser.disable_interspersed_args()

    return parser


def main(argv=None):

    parser = get_option_parser()

    (options, args) = E.start(parser, add_cluster_options=True)

    if len(args) == 0:
        raise ValueError(
            "command line argument missing - see usage information")

    options.renumber_column = [x.split(":") for x in options.renumber_column]

    cmd = args[0]
    if len(args) > 1:
        cmd += " '" + "' '".join(args[1:]) + "'"

    if options.dry_run:

        cmd = re.sub("%DIR%", "", cmd)
        retcode = subprocess.call(cmd,
                                  shell=True,
                                  stdin=sys.stdin,
                                  stdout=sys.stdout,
                                  cwd=os.getcwd(),
                                  close_fds=True)
        E.stop()
        sys.exit(0)

    failed_requests = []
    started_requests = []
    niterations = 0

    P.get_parameters()
    P.start_session()

    if not options.collect:
        tmpdir = os.path.abspath(tempfile.mkdtemp(dir=options.tmpdir))

        E.info(" working in directory %s" % tmpdir)

        if options.split_at_lines:
            chunk_iterator = chunk_iterator_lines
            args = (options.split_at_lines,)
        elif options.split_at_column:
            chunk_iterator = chunk_iterator_column
            args = (options.split_at_column - 1, options.max_files)
        elif options.split_at_regex:
            chunk_iterator = chunk_iterator_regex_split
            args = (re.compile(options.split_at_regex),
                    0,
                    options.chunksize,
                    options.max_lines)
        elif options.group_by_regex:
            chunk_iterator = chunk_iterator_regex_group
            args = (re.compile(options.group_by_regex), 0, options.chunksize)
        else:
            raise ValueError("please specify a way to chunk input data")

        data = [(x, cmd, options, None, options.subdirs)
                for x in chunk_iterator(
                    options.stdin,
                    args,
                    prefix=tmpdir,
                    use_header=options.input_header)]

        statements = [build_command(x) for x in data]
        started_requests = [(x[0], x[0] + ".out") for x in data]

        if len(data) == 0:
            E.warn("no data received")
            E.stop()
            sys.exit(0)

        P.run(statements)
    else:
        tmpdir = options.collect
        started_requests = [(x[:-4], x) for x in glob.glob(tmpdir + "/*.out")]

        E.info("collecting %i files from %s" % (len(started_requests),
                                                tmpdir))

    if failed_requests:
        for fn, cmd in failed_requests:
            E.error("failed request: filename= %s, cmd= %s" % (fn, cmd))
    else:
        E.info("building result from %i parts" % len(started_requests))

        if options.renumber:
            mapper = MapperLocal(pattern=options.renumber)
        else:
            mapper = MapperEmpty()

        # deal with stdout
        name = None
        index = None

        for pattern, column in options.renumber_column:

            if re.search(pattern, "stdout"):
                try:
                    index = int(column) - 1
                except ValueError:
                    name = column
                    break

        if options.binary:
            ResultBuilderBinary()(started_requests, options.stdout, options)
        else:
            regex = None
            if options.output_regex_header:
                regex = re.compile(options.output_regex_header)
            ResultBuilder(mapper=mapper,
                          field_index=index,
                          field_name=name,
                          header_regex=regex
                          )(started_requests, options.stdout, options)

        # deal with logfiles : combine them into a single file
        rr = re.search(r"'--log=(\S+)'", cmd) or re.search(r"'--L\s+(\S+)'", cmd)
        if rr:
            E.info("logging output goes to %s" % rr.groups()[0])
            logfile = iotools.open_file(rr.groups()[0], "a")
            ResultBuilderLog()(
                [(x[0], "%s.log" % x[0]) for x in started_requests],
                logfile,
                options)
            logfile.close()

        # deal with other files
        if options.subdirs:

            files = glob.glob("%s/*.dir/*" % tmpdir)
            # remove directory
            filenames = set([os.path.basename(x) for x in files])
            xx = len(".out")

            for filename in filenames:

                _, filetype = os.path.splitext(filename)

                name = None
                index = None

                for pattern, column in options.renumber_column:
                    if re.search(pattern, filename):
                        try:
                            index = int(column) - 1
                        except ValueError:
                            name = column
                        break

                if options.binary:
                    builder = ResultBuilderBinary(mapper=mapper)
                elif filetype in (".fa", ".fasta"):
                    builder = ResultBuilderFasta(mapper=mapper)
                elif filetype in (".mali", ):
                    builder = ResultBuilderFasta(mapper=MapperEmpty())
                elif filetype in (".png"):
                    builder = ResultBuilderCopies(mapper=mapper)
                else:
                    builder = ResultBuilder(
                        mapper=mapper, field_index=index, field_name=name)

                E.debug("chose the following builder for %s: %s: %s" %
                        (filename, filetype, str(builder)))

                E.info("collecting results for %s" % filename)

                input_filenames = []
                for fi, fn in started_requests:
                    fn = fn[:-xx] + ".dir/" + filename
                    if os.path.exists(fn):
                        input_filenames.append((fi, fn))

                E.info("output of %i files goes to %s" %
                       (len(filenames), filename))

                outfile = iotools.open_file(
                    options.output_pattern % filename, "w")
                builder(input_filenames, outfile, options)
                outfile.close()

    if not options.debug and (not options.resume or not options.collect):
        if len(failed_requests) == 0:
            E.info("removing directory %s" % tmpdir)
            shutil.rmtree(tmpdir)
        else:
            E.info("directory %s not removed due to %i failed jobs" %
                   (tmpdir, len(failed_requests)))

    E.info("job control: nstarted=%i, nfinished=%i, nerrors=%i, nrepeats=%i" %
           (len(started_requests),
            len(started_requests) - len(failed_requests),
            len(failed_requests),
            niterations))

    E.stop()


if __name__ == '__main__':
    sys.exit(main())
