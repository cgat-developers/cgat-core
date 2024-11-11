"""===========================
pipeline template
===========================

.. Replace the documentation below with your own description of the
   pipeline's purpose

This script is a template for writing a workflow using the cgatcore
toolkit.

Overview
========

Usage
=====

To run this pipeline, type::

   cgatcore template-pipeline make all

Input files
-----------

None required.

pipeline output
===============

.. Describe output files of the pipeline here

Code
====

"""

import sys
import ruffus
import numpy.random
import cgatcore.experiment as E
from cgatcore import pipeline as P


def create_files(outfile):

    E.debug("creating output file {}".format(outfile))
    with open(outfile, "w") as outf:
        outf.write("\n".join(map(
            str,
            numpy.random.normal(
                P.get_params()["mu"],
                P.get_params()["sigma"],
                P.get_params()["num_samples"]))) + "\n")


def compute_mean(infile, outfile):
    """compute mean"""

    statement = (
        "cat %(infile)s "
        "| awk '$1 > %(min_value)f "
        "{{a += $1}} END {{print a/NR}}' "
        "> %(outfile)s".format(**locals()))

    P.run(statement)


def combine_means(infiles, outfile):
    infiles = " ".join(infiles)
    statement = (
        "cat %(infiles)s "
        "> %(outfile)s ".format(**locals()))
    P.run(statement)


def main(argv=None):
    if argv is None:
        argv = sys.argv

    args = P.initialize(argv,
                        config_file="template.yml",
                        defaults={
                            "min_value": 0.0,
                            "num_samples": 1000,
                            "mu": 0.0,
                            "sigma": 1.0})

    pipeline = ruffus.Pipeline("template_pipeline")

    task_create_files = pipeline.originate(
        task_func=create_files,
        output=["sample_{:02}.txt".format(x) for x in range(10)])

    task_compute_mean = pipeline.transform(
        task_func=compute_mean,
        input=task_create_files,
        filter=ruffus.suffix(".txt"),
        output=".mean")

    task_combine_means = pipeline.merge(
        task_func=combine_means,
        input=task_compute_mean,
        output="means.txt")

    # primary targets
    pipeline.merge(
        task_func=P.EmptyRunner("all"),
        input=task_combine_means,
        output="all")

    E.debug("starting workflow")
    return P.run_workflow(args, argv)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
