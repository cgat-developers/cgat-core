.. _defining_workflow-Configuration:


============================
Writing a workflow- Tutorial
============================

The explicit aim of CGAT-core is to allow users to quickly and easily build their own computational pipelines that will speed up your analysis workflow.

However, we also have bioler plate code that helps automate and  build computational pipelines more easily.
You can find this template here: LINK!!! This script creates a new pipeline according to the CGAT
pipeline layout and is useful when starting a new pipeline from scratch.

**Installation of CGAT code**

In order to begin writing a pipeline you will need to install the CGATCore code 
(see :ref:`getting_started-Installation`) for installation instructions.

To run pipeline_quickstart.py (thecode to generate the boiler plate layout) please install the
quickstart pipeline from CGAT-flow by following these instructions::

   Add code steps here

Tutorial start
--------------

Setting up the pipleine
=======================

**1.** First navigate to a directory where you want to run your code and run quickstart::

   cd /location/where/you/want/the/analysis/performed
   cgatflow quickstart --set-name=test

This will create a directory called test in the current directory with the following layout::

   |-- work
   |   |-- conf.py -> ../src/pipeline_test/conf.py
   |   `-- pipeline.ini -> ../src/pipeline_test/pipeline.ini
   `-- src
       |-- pipeline_test
       |   conf.py
       |   pipeline.ini
       |-- pipeline_test.py
       `-- pipeline_docs


The layout has the following components::

work
   Directory for running the pipeline. Links to configuration files in
   the src directory. This directory exists to separate code
   from data and results.
src
   Directory for pipeline code. This directory should be put under
   version control and backed-up.
src/pipeline_test.py
   The main pipeline script
src/pipeline_test/
   Directory for the pipeline configuration files
src/pipeline_docs/pipeline_test/
   Directory for the CGAT pipeline report - CGATReport is no longer supported and is here for legacy reasons
src/pipeline_docs/themes
   The CGAT report theme - CGATReport is no longer supported and is here for legacy reasons

**2.** View the source code within src/pipeline_test.py

This is the code that you use to write your pipeline with. The code begins with a doc
string detailing the pipeline functionality. You should use this to document your
pipeline. It also contains utility functions that help wth executing the pipeline.

**Config parser:** This code helps with parsing the pipeline.ini file::

    # load options from the config file
    PARAMS = P.getParameters(
        ["%s/pipeline.ini" % os.path.splitext(__file__)[0],
     	"../pipeline.ini",
     	"pipeline.ini"])

**Database helper:** This code helps with connecting to a sqlite database::

    def connect():
	 '''utility function to connect to database.

	 Use this method to connect to the pipeline database.
	 Additional databases can be attached here as well.

	 Returns an sqlite3 database handle.
	 '''

	 dbh = sqlite3.connect(PARAMS["database_name"])
	 statement = '''ATTACH DATABASE '%s' as annotations''' % (
	     PARAMS["annotations_database"])
	 cc = dbh.cursor()
	 cc.execute(statement)
	 cc.close()

	 return dbh


**Commandline parser:** This bit of code allows pipeline to parse arguments and run the pipleine using the 
`cgatflow` command::

    def main(argv=None):
	if argv is None:
	    argv = sys.argv
	P.main(argv)


    if __name__ == "__main__":
	sys.exit(P.main(sys.argv))    



Running quickstart pipeline
===========================

The code that is generated using cgatflow quickstart contains two ruffus_
**@transform** tasks that parse the pipeline.ini and conf.py files (see code below). The first function
called :code:`countWords` is then called which contains a statement that counts the
number of words in the file. The statement is then ran using :code:`P.run()` function.

The second ruffus_ **@transform** function called :code:`loadWordCounts` takes as an input the output of
the function countWords and loads the number of words to a sqlite database using :code:`P.load()`.

The third :code:`def full()` function is a dummy task that is written to run the whole
pipeline. It has an **@follows** function that takes the :code:`loadWordCounts` function.
This helps complete the pipeline chain and the pipeline can be ran with the tak name full to execute the
whole workflow.
::  

   # ---------------------------------------------------
   # Specific pipeline tasks
   @transform(("pipeline.ini", "conf.py"),
	      regex("(.*)\.(.*)"),
	      r"\1.counts")
   def countWords(infile, outfile):
       '''count the number of words in the pipeline configuration files.'''

       # the command line statement we want to execute
       statement = '''awk 'BEGIN { printf("word\\tfreq\\n"); } 
       {for (i = 1; i <= NF; i++) freq[$i]++}
       END { for (word in freq) printf "%%s\\t%%d\\n", word, freq[word] }'
       < %(infile)s > %(outfile)s'''

       # execute command in variable statement.
       #
       # The command will be sent to the cluster.  The statement will be
       # interpolated with any options that are defined in in the
       # configuration files or variable that are declared in the calling
       # function.  For example, %(infile)s will we substituted with the
       # contents of the variable "infile".
       P.run()


   @transform(countWords,
	      suffix(".counts"),
	      "_counts.load")
   def loadWordCounts(infile, outfile):
       '''load results of word counting into database.'''
       P.load(infile, outfile, "--add-index=word")

   # ---------------------------------------------------
   # Generic pipeline tasks
   @follows(loadWordCounts)
   def full():
       pass

To run the pipeline navigate to the working directory and then run the cgatflow
command.
::

   cd Work/
   cgatflow test make full

The pipeline will then execute and count the words in the ini and conf.py files.


Modifying quickstart to build your own workflows
================================================

The next step is to modify the basic code in the pipeline to fit your particular
NGS workflow needs. For example, say we wanted to convert a sam file into a bam
file then perform flag stats on that output bam file. The basic quickstart pipeline
framework can be easily modified to perform this. We would remove all of the code under
the Specific pipeline tasks and write our own.

The first step would be to add a first function to the pipeline to identify all
sam file in a data.dir directory. This first function would accept a sam file then
use samtools view to convert it to a bam file. 

The second function would then take the output of the first function and perform samtools
flagstat and output the results as a flat .txt file.

This would be written as follows and the :code:`bamFlagstats` would be added as a @follows()
parameter in the full task. i.e. :code:`@follows(bamFlagstats)`.
::
   @transform("data.dir/*.sam",
	      regex("data.dir/(\S+).sam"),
	      r"\1.bam")
   def bamConvert(infile, outfile):
       'convert a sam file into a bam file using samtools view'

       statement = ''' samtools view -bT /ifs/mirror/genomes/plain/hg19.fasta 
                       %(infile)s > %(outfile)s'''

       P.run()

   @transform(bamConvert,
	      suffix(".bam"),
	      "_flagstats.txt")
   def bamFlagstats(infile, outfile):
       'perform flagstats on a bam file'

       statement = '''samtools flagstat %(infile)s > %(outfile)s'''

       P.run()


To run the pipeline::

    cgatflow make full


The bam files and flagstats outputs should then be generated.


Parameterising the code using the .ini file
===========================================

Having written the basic function of our pipleine, as a philosophy,
we try and avoid any hard coded parameters.

This means that any variables can be easily modified by the user
without having to modify any code.

Looking at the code above, the hard coded link to the hg19.fasta file
can be added as a customisable parameter. This could allow the user to
specify any fasta file depending on the genome build used to map and 
generate the bam file.

In order to do this the :file:`pipeline.ini` file needs to be modifiedand this
can be performed in the following way:

Configuration values are accessible via the :py:data:`PARAMS`
variable. The :py:data:`PARAMS` variable is a dictionary mapping
configuration parameters to values. Keys are in the format
``section_parameter``. For example, the key ``genome_fasta`` will
provide the configuration value of::

    [genome]
    fasta=/ifs/mirror/genomes/plain/hg19.fasta

In the pipeline.ini, add the above code to the file and in the pipeline_test.py
code the value can be accessed via ``PARAMS["genome_fasta"]``.

Therefore the code we wrote before for parsing bam files can be modified to
::
   @transform("data.dir/*.sam",
	      regex("data.dir/(\S+).sam"),
	      r"\1.bam")
   def bamConvert(infile, outfile):
       'convert a sam file into a bam file using samtools view'

       genome_fasta = PARAMS["genome_fasta"]

       statement = ''' samtools view -bT  %(genome_fasta)s
                       %(infile)s > %(outfile)s'''

       P.run()

   @transform(bamConvert,
	      suffix(".bam"),
	      "_flagstats.txt")
   def bamFlagstats(infile, outfile):
       'perform flagstats on a bam file'

       statement = '''samtools flagstat %(infile)s > %(outfile)s'''

       P.run()


Running the code again should generate the same output but if next time you
had bam files that came from a different genome build then the parameter in the ini file
can be modified easily.
