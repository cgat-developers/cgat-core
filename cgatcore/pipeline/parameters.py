"""Parameters.py - Parameter handling for ruffus pipelines
==========================================================

Reference
---------

"""

import collections
import os
import sys
import platform
import getpass
import logging
import re
from collections import defaultdict
try:
    from collections.abc import Sequence, Mapping  # noqa
except ImportError:
    from collections import Sequence, Mapping  # noqa

import yaml
import cgatcore.experiment as E
import cgatcore.iotools as iotools
from cgatcore.pipeline.utils import get_caller_locals, is_test

HAVE_INITIALIZED = False

# sort out script paths

# root directory of code
SCRIPTS_ROOT_DIR = os.path.dirname(
    os.path.dirname(E.__file__))

# script directory
SCRIPTS_SCRIPTS_DIR = os.path.join(SCRIPTS_ROOT_DIR, "scripts")

# if pipeline.py is called from an installed version, scripts are
# located in the "bin" directory.
if not os.path.exists(SCRIPTS_SCRIPTS_DIR):
    SCRIPTS_DIR = os.path.join(sys.exec_prefix, "bin")


def get_logger():
    return logging.getLogger("cgatcore.pipeline")


class TriggeredDefaultFactory:
    with_default = False

    def __call__(self):
        if TriggeredDefaultFactory.with_default:
            return str()
        else:
            raise KeyError("missing parameter accessed")


# Global variable for parameter interpolation in commands
# This is a dictionary that can be switched between defaultdict
# and normal dict behaviour.
PARAMS = defaultdict(TriggeredDefaultFactory())

# patch - if --help or -h in command line arguments,
# switch to a default dict to avoid missing paramater
# failures
if is_test() or "--help" in sys.argv or "-h" in sys.argv:
    TriggeredDefaultFactory.with_default = True

# A list of hard-coded parameters for fall-back. These should be
# overwritten by command line options and configuration files.
HARDCODED_PARAMS = {
    'scriptsdir': SCRIPTS_SCRIPTS_DIR,
    'toolsdir': SCRIPTS_SCRIPTS_DIR,
    # directory used for temporary local files
    # on the submission node
    'tmpdir': os.environ.get("TMPDIR",
                             os.path.join("/tmp", getpass.getuser())),
    # directory used for temporary files shared across machines
    'shared_tmpdir': os.environ.get("SHARED_TMPDIR", os.path.abspath(os.getcwd())),
    # database backend
    'database': {'url': 'sqlite:///./csvdb'},
    # cluster options - parameterized for CGAT cluster for testing
    'cluster': {
        # cluster queue to use
        'queue': 'all.q',
        # priority of jobs in cluster queue
        'priority': -10,
        # number of jobs to submit to cluster queue
        'num_jobs': 100,
        # name of consumable resource to use for requesting memory
        'memory_resource': "mem_free",
        # amount of memory set by default for each job
        'memory_default': "4G",
        # ensure requested memory is not exceeded via ulimit (this is
        # not compatible with and/or needed  for all cluster configurations)
        'memory_ulimit': False,
        # general cluster options
        'options': "",
        # parallel environment to use for multi-threaded jobs
        'parallel_environment': 'dedicated',
        # the cluster queue manager
        'queue_manager': "sge",
        # directory specification for temporary files on cluster nodes,
        # useful for passing the name of a non-standard environment variable
        # which will be set by the queue manager e.g. in a prolog script
        # if set to False, the general "tmpdir" parameter is used.
        'tmpdir': False
    },
    # ruffus job limits for databases
    'jobs_limit_db': 10,
    # ruffus job limits for R
    'jobs_limit_R': 1,
    # operating system we are running on
    'os': platform.system(),
    # set start and work directory at process launch to prevent
    # repeated calls to os.getcwd failing if network is busy
    "start_dir": os.getcwd(),
    "work_dir": os.getcwd()
}

# After all configuration files have been read, some
# parameters need to be interpolated with other parameters
# The list is below:
INTERPOLATE_PARAMS = []


def config_to_dictionary(config):
    """convert the contents of a :py:class:`ConfigParser.ConfigParser`
    object to a dictionary

    This method works by iterating over all configuration values in a
    :py:class:`ConfigParser.ConfigParser` object and inserting values
    into a dictionary. Section names are prefixed using and underscore.
    Thus::

        [sample]
        name=12

    is entered as ``sample_name=12`` into the dictionary. The sections
    ``general`` and ``DEFAULT`` are treated specially in that both
    the prefixed and the unprefixed values are inserted: ::

       [general]
       genome=hg19

    will be added as ``general_genome=hg19`` and ``genome=hg19``.

    Numbers will be automatically recognized as such and converted into
    integers or floats.

    Returns
    -------
    config : dict
        A dictionary of configuration values

    """
    p = defaultdict(lambda: defaultdict(TriggeredDefaultFactory()))
    for section in config.sections():
        for key, value in config.items(section):
            try:
                v = iotools.str2val(value)
            except TypeError:
                E.error("error converting key %s, value %s" % (key, value))
                E.error("Possible multiple concurrent attempts to "
                        "read configuration")
                raise

            p["%s_%s" % (section, key)] = v

            # IMS: new heirarchical format
            try:
                p[section][key] = v
            except TypeError:
                # fails with things like genome_dir=abc
                # if [genome] does not exist.
                continue

            if section in ("general", "DEFAULT"):
                p["%s" % (key)] = v

    for key, value in config.defaults().items():
        p["%s" % (key)] = iotools.str2val(value)

    return p


def nested_update(old, new):
    '''Update potentially nested dictionaries. If both old[x] and new[x]
    inherit from collections.Mapping, then update old[x] with entries from
    new[x], otherwise set old[x] to new[x]'''

    for key, value in new.items():
        if isinstance(value, Mapping) and \
           isinstance(old.get(key, str()), Mapping):
            old[key].update(new[key])
        else:
            old[key] = new[key]


def input_validation(PARAMS, pipeline_script=""):
    '''Inspects the PARAMS dictionary looking for problematic input values.

    So far we just check that:

        * all required 3rd party tools are on the PATH

        * input parameters are not empty

        * input parameters do not contain the "?" character (used as a
          placeholder in different pipelines)

        * if the input is a file, check whether it exists and
          is readable
    '''

    E.info('''input Validation starting''')
    E.info('''checking 3rd party dependencies''')

    # check 3rd party dependencies
    if len(pipeline_script) > 0:
        # this import requires the PYTHONPATH in the following order
        # PYTHONPATH=<src>/CGATpipelines:<src>/cgat
        import scripts.cgat_check_deps as cd
        deps, check_path_failures = cd.checkDepedencies(pipeline_script)
        # print info about dependencies
        if len(deps) == 0:
            E.info('no dependencies found')
        else:
            # print dictionary ordered by value
            for k in sorted(deps, key=deps.get, reverse=True):
                E.info('Program: {0!s} used {1} time(s)'.format(k, deps[k]))
            n_failures = len(check_path_failures)
            if n_failures == 0:
                E.info('All required programs are available on your PATH')
            else:
                E.info('The following programs are not on your PATH')
                for p in check_path_failures:
                    E.info('{0!s}'.format(p))

    # check PARAMS
    num_missing = 0
    num_questions = 0

    E.info('''checking pipeline configuration''')

    for key, value in sorted(PARAMS.iteritems()):

        key = str(key)
        value = str(value)

        # check for missing values
        if value == "":
            E.warn('\n"{}" is empty, is that expected?'.format(key))
            num_missing += 1

        # check for a question mark in the dictironary (indicates
        # that there is a missing input parameter)
        if "?" in value:
            E.warn('\n"{}" is not defined (?), is that expected?'.format(key))
            num_questions += 1

        # validate input files listed in PARAMS
        if (value.startswith("/") or value.endswith(".gz") or value.endswith(".gtf")) and "," not in value:
            if not os.access(value, os.R_OK):
                E.warn('\n"{}": "{}" is not readable'.format(key, value))

    if num_missing or num_questions:
        raise ValueError("pipeline has configuration issues")


def get_parameters(filenames=None,
                   defaults=None,
                   site_ini=True,
                   user=True,
                   only_import=None):
    '''read one or more config files and build global PARAMS configuration
    dictionary.

    Arguments
    ---------
    filenames : list
       List of filenames of the configuration files to read.
    defaults : dict
       Dictionary with default values. These will be overwrite
       any hard-coded parameters, but will be overwritten by user
       specified parameters in the configuration files.
    user : bool
       If set, configuration files will also be read from a
       file called :file:`.cgat.yml` in the user`s
       home directory.
    only_import : bool
       If set to a boolean, the parameter dictionary will be a
       defaultcollection. This is useful for pipelines that are
       imported (for example for documentation generation) but not
       executed as there might not be an appropriate .yml file
       available. If `only_import` is None, it will be set to the
       default, which is to raise an exception unless the calling
       script is imported or the option ``--is-test`` has been passed
       at the command line.

    Returns
    -------
    params : dict
       Global configuration dictionary.
    '''
    global PARAMS, HAVE_INITIALIZED
    # only execute function once
    if HAVE_INITIALIZED:
        return PARAMS

    if filenames is None:
        filenames = ["pipeline.yml"]

    if isinstance(filenames, str):
        filenames = [filenames]

    old_id = id(PARAMS)

    caller_locals = get_caller_locals()

    # check if this is only for import
    if only_import is None:
        only_import = is_test() or "__name__" not in caller_locals or \
                      caller_locals["__name__"] != "__main__"

    # important: only update the PARAMS variable as
    # it is referenced in other modules. Thus the type
    # needs to be fixed at import. Raise error where this
    # is not the case.
    # Note: Parameter sharing in the pipeline module needs
    # to be reorganized.
    if only_import:
        # turn on default dictionary
        TriggeredDefaultFactory.with_default = True

    # check if the pipeline is in testing mode
    found = False
    if 'argv' in caller_locals and caller_locals['argv'] is not None:
        for e in caller_locals['argv']:
            if 'template_pipeline.py' in e:
                found = True
    PARAMS['testing'] = 'self' in caller_locals or found

    if site_ini:
        # read configuration from /etc/cgat/pipeline.yml
        fn = "/etc/cgat/pipeline.yml"
        if os.path.exists(fn):
            filenames.insert(0, fn)

    if user:
        # read configuration from a users home directory
        fn = os.path.join(os.path.expanduser("~"),
                          ".cgat.yml")
        if os.path.exists(fn):
            if 'pipeline.yml' in filenames:
                index = filenames.index('pipeline.yml')
                filenames.insert(index, fn)
            else:
                filenames.append(fn)

    filenames = [x.strip() for x in filenames if os.path.exists(x)]

    # save list of config files
    PARAMS["pipeline_yml"] = filenames

    # update with hard-coded PARAMS
    nested_update(PARAMS, HARDCODED_PARAMS)
    if defaults:
        nested_update(PARAMS, defaults)

    # reset working directory. Set in PARAMS to prevent repeated calls to
    # os.getcwd() failing if network is busy
    PARAMS["start_dir"] = os.path.abspath(os.getcwd())
    # location of pipelines - set via location of top frame (cgatflow command)
    if '__file__' in caller_locals:
        PARAMS["pipelinedir"] = os.path.dirname(caller_locals["__file__"])
    else:
        PARAMS["pipelinedir"] = 'unknown'

    for filename in filenames:
        if not os.path.exists(filename):
            continue
        get_logger().info("reading config from file {}".format(
            filename))

        with open(filename, 'rt', encoding='utf8') as inf:
            p = yaml.load(inf, Loader=yaml.FullLoader)
            if p:
                nested_update(PARAMS, p)

    # for backwards compatibility - normalize dictionaries
    p = {}
    for k, v in PARAMS.items():
        if isinstance(v, Mapping):
            for kk, vv in v.items():
                new_key = "{}_{}".format(k, kk)
                if new_key in p:
                    raise ValueError(
                        "key {} does already exist".format(new_key))
                p[new_key] = vv
    nested_update(PARAMS, p)

    # interpolate some params with other parameters
    for param in INTERPOLATE_PARAMS:
        try:
            PARAMS[param] = PARAMS[param] % PARAMS
        except TypeError as msg:
            raise TypeError('could not interpolate %s: %s' %
                            (PARAMS[param], msg))

    # expand directory pathnames
    for param, value in list(PARAMS.items()):
        if (param.endswith("dir") and isinstance(value, str) and value.startswith(".")):
            PARAMS[param] = os.path.abspath(value)

    # make sure that the dictionary reference has not changed
    assert id(PARAMS) == old_id
    HAVE_INITIALIZED = True
    return PARAMS


def match_parameter(param):
    '''find an exact match or prefix-match in the global
    configuration dictionary param.

    Arguments
    ---------
    param : string
        Parameter to search for.

    Returns
    -------
    name : string
        The full parameter name.

    Raises
    ------
    KeyError if param can't be matched.

    '''
    if param in PARAMS:
        return param

    for key in list(PARAMS.keys()):
        if "%" in key:
            rx = re.compile(re.sub("%", ".*", key))
            if rx.search(param):
                return key

    raise KeyError("parameter '%s' can not be matched in dictionary" %
                   param)


def substitute_parameters(**kwargs):
    '''return a parameter dictionary.

    This method builds a dictionary of parameter values to
    apply for a specific task. The dictionary is built in
    the following order:

    1. take values from the global dictionary (:py:data:`PARAMS`)
    2. substitute values appearing in `kwargs`.
    3. Apply task specific configuration values by looking for the
       presence of ``outfile`` in kwargs.

    The substition of task specific values works by looking for any
    parameter values starting with the value of ``outfile``.  The
    suffix of the parameter value will then be substituted.

    For example::

        PARAMS = {"tophat_threads": 4,
                  "tophat_cutoff": 0.5,
                  "sample1.bam.gz_tophat_threads" : 6}
        outfile = "sample1.bam.gz"
        print(substitute_parameters(**locals()))
        {"tophat_cutoff": 0.5, "tophat_threads": 6}

    Returns
    -------
    params : dict
        Dictionary with parameter values.

    '''

    # build parameter dictionary
    # note the order of addition to make sure that kwargs takes precedence
    local_params = dict(list(PARAMS.items()) + list(kwargs.items()))

    if "outfile" in local_params:
        # replace specific parameters with task (outfile) specific parameters
        outfile = local_params["outfile"]
        keys = list(local_params.keys())
        for k in keys:
            if k.startswith(outfile):
                p = k[len(outfile) + 1:]
                if p not in local_params:
                    # do not raise error, argument might be a prefix
                    continue
                get_logger.debug("substituting task specific parameter "
                                 "for %s: %s = %s" %
                                 (outfile, p, local_params[k]))
                local_params[p] = local_params[k]

    return local_params


def as_list(value):
    '''return a value as a list.

    If the value is a string and contains a ``,``, the string will
    be split at ``,``.

    Returns
    -------
    list

    '''
    if type(value) == str:
        try:
            values = [x.strip() for x in value.strip().split(",")]
        except AttributeError:
            values = [value.strip()]
        return [x for x in values if x != ""]
    elif type(value) in (list, tuple):
        return value
    else:
        return [value]


def is_true(param, **kwargs):
    '''return True if param has a True value.

    A parameter is False if it is:

    * not set
    * 0
    * the empty string
    * false or False

    Otherwise the value is True.

    Arguments
    ---------
    param : string
        Parameter to be tested
    kwargs : dict
        Dictionary of local configuration values. These will be passed
        to :func:`substitute_parameters` before evaluating `param`

    Returns
    -------
    bool

    '''
    if kwargs:
        p = substitute_parameters(**kwargs)
    else:
        p = PARAMS
    value = p.get(param, 0)
    return value not in (0, '', 'false', 'False')


def check_parameter(param):
    """check if parameter ``key`` is set"""
    if param not in PARAMS:
        raise ValueError("need `%s` to be set" % param)


def get_params():
    """return handle to global parameter dictionary"""
    return PARAMS


def get_parameters_as_namedtuple(*args, **kwargs):
    """return PARAM dictionary as a namedtuple.
    """
    d = get_parameters(*args, **kwargs)
    return collections.namedtuple('GenericDict', list(d.keys()))(**d)


def get_param_section(section):
    """return config values in section

    Sections are built by common prefixes.
    """
    if not section.endswith("_"):
        section = section + "_"
    n = len(section)
    return [(x[n:], y) for x, y in PARAMS.items() if x.startswith(section)]
