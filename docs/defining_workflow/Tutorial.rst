.. _defining_workflow-Configuration:


============================
Writing a workflow- Tutorial
============================

The explicit aim of cgat-core is to allow users to quickly and easily build their own computational pipelines that will speed up your analysis workflow.

Installation of cgat-core
-------------------------

In order to begin writing a pipeline you will need to install the cgat-core code 
(see :ref:`getting_started-Installation`) for installation instructions.


Tutorial start
--------------

Setting up the pipleine
=======================

**1.** First navigate to a directory where you want to start building your code::

   mkdir test && cd test && mkdir configuration && touch configuration/pipeline.yml && touch pipeline_test.py && touch ModuleTest.py

This will create a directory called test in the current directory with the following layout::

   |-- configuration
   |   `-- pipeline.yml
   `-- pipeline_test.py
    -- ModuleTest.py


The layout has the following components::

pipeline_test.py
   This is the file that will contain all of the ruffus workflows, the file needs
   the format pipeline_<name>.py
test/
   Directory containing the configuration yml file. The directory needs to be named
   the same as the pipeline_<name>.py file. This folder will contain the `pipeline.yml`
   configuration file.
ModuleTest.py
   This file will contain functions that will be imported into the main ruffus
   workflow file, pipeline_test.py

**2.** View the source code within pipeline_test.py

This is where the ruffus tasks will be written. The code begins with a doc
string detailing the pipeline functionality. You should use this section to document your
pipeline. ::

    '''This pipeline is a test and this is where the documentation goes '''

The pipeline then needs a few utility functions to help with executing the pipeline.

**Import statements** You will need to import ruffus and cgatcore utilities ::

  from ruffus import *
  import cgatcore.experiment as E
  from cgatcore import pipeline as P

Importing ruffus allows ruffus decorators to be used within out pipeline

Importing experiment from cgatcore is a module that contains ultility functions for argument parsion, logging and record keeping
within scripts.

Importing pipeline from cgatcore allows users to run utility functions for interfacing with CGAT ruffus pipelines
with an HPC cluster, uploading data to a database, provides paramaterisation and more.

You'll also need python modules::
  
  import os
  import sys

**Config parser:** This code helps with parsing the pipeline.yml file::

    # load options from the config file
    PARAMS = P.get_parameters(
        ["%s/pipeline.yml" % os.path.splitext(__file__)[0],
     	"../pipeline.yml",
     	"pipeline.yml"])

**Pipeline configuration:** We will add configurable variables to our pipeline.yml file
so that we can modify the output of out pipeline. With `pipeline.yml` open, copy and paste the following
into the file. ::

	database:
	   name: "csvdb"
	   
When you come to run the pipeline the configuration variables (in this case csvdb) can be accessed in the pipeline
by PARAMS["database_name"].


**Database connection:** This code helps with connecting to a sqlite database::

    def connect():
	 '''utility function to connect to database.

	 Use this method to connect to the pipeline database.
	 Additional databases can be attached here as well.

	 Returns an sqlite3 database handle.
	 '''

	 dbh = sqlite3.connect(PARAMS["database_name"])

	 return dbh


**Commandline parser:** This bit of code allows pipeline to parse arguments. ::

    def main(argv=None):
	if argv is None:
	    argv = sys.argv
	P.main(argv)


    if __name__ == "__main__":
	sys.exit(P.main(sys.argv))    
	

Running test pipeline
=====================

You now have the bare bones layout of the pipeline and you now need code to execute. Below you will
find example code that you can copy and paste into your pipeline_test.py file. The code 
includes two ruffus_ **@transform** tasks that parse the pipeline.yml. The first function
called :code:`countWords` is then called which contains a statement that counts the
number of words in the file. The statement is then ran using :code:`P.run()` function.

The second ruffus_ **@transform** function called :code:`loadWordCounts` takes as an input the output of
the function countWords and loads the number of words to a sqlite database using :code:`P.load()`.

The third :code:`def full()` function is a dummy task that is written to run the whole
pipeline. It has an **@follows** function that takes the :code:`loadWordCounts` function.
This helps complete the pipeline chain and the pipeline can be ran with the tak name full to execute the
whole workflow.

The following code should be pasted just before the **Commandline parser** arguments and after the **database connection** code.
::  

   # ---------------------------------------------------
   # Specific pipeline tasks
   @transform("pipeline.yml",
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
       P.run(statement)


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

To run the pipeline navigate to the working directory and then run the pipeline. ::

   python /location/to/code/pipeline_test.py config
   python /location/to/code/pipeline_test.py show full -v 5

This will place the pipeline.yml in the folder. Then run ::

   python /location/to/code/pipeline_test.py  make full -v5 --local

The pipeline will then execute and count the words in the yml file.


Modifying the test pipeline to build your own workflows
=======================================================

The next step is to modify the basic code in the pipeline to fit your particular
NGS workflow needs. For example, say we wanted to convert a sam file into a bam
file then perform flag stats on that output bam file. The code and layout that we just wrote 
can be easily modified to perform this. We would remove all of the code from the 
specific pipeline tasks and write our own.

The pipeline will have two steps: 
1. Identify all sam files and convert to a bam file. 
2. Take the output of step 1 and then perform flagstats on that bam file.

The first step would involve writing a function to identify all
`sam` files in a `data.dir/` directory. This first function would accept a sam file then
use samtools view to convert it to a bam file. Therefore, we would require an ``@transform``
function.

The second function would then take the output of the first function, perform samtools
flagstat and then output the results as a flat .txt file. Again, an ``@transform`` function is required
to track the input and outputs.

This would be written as follows:
::
   @transform("data.dir/*.sam",
	      regex("data.dir/(\S+).sam"),
	      r"\1.bam")
   def bamConvert(infile, outfile):
       'convert a sam file into a bam file using samtools view'

       statement = ''' samtools view -bT /ifs/mirror/genomes/plain/hg19.fasta 
                       %(infile)s > %(outfile)s'''

       P.run(statement)

   @transform(bamConvert,
	      suffix(".bam"),
	      "_flagstats.txt")
   def bamFlagstats(infile, outfile):
       'perform flagstats on a bam file'

       statement = '''samtools flagstat %(infile)s > %(outfile)s'''

       P.run(statement)


To run the pipeline::

    python /path/to/file/pipeline_test.py make full -v5


The bam files and flagstats outputs should then be generated.


Parameterising the code using the .yml file
===========================================

Having written the basic function of our pipleine, as a philosophy,
we try and avoid any hard coded parameters.

This means that any variables can be easily modified by the user
without having to modify any code.

Looking at the code above, the hard coded link to the hg19.fasta file
can be added as a customisable parameter. This could allow the user to
specify any fasta file depending on the genome build used to map and 
generate the bam file.

In order to do this the :file:`pipeline.yml` file needs to be modified. This
can be performed in the following way:

Configuration values are accessible via the :py:data:`PARAMS`
variable. The :py:data:`PARAMS` variable is a dictionary mapping
configuration parameters to values. Keys are in the format
``section_parameter``. For example, the key ``genome_fasta`` will
provide the configuration value of::

    genome:
        fasta: /ifs/mirror/genomes/plain/hg19.fasta

In the pipeline.yml, add the above code to the file. In the pipeline_test.py
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

       P.run(statement)

   @transform(bamConvert,
	      suffix(".bam"),
	      "_flagstats.txt")
   def bamFlagstats(infile, outfile):
       'perform flagstats on a bam file'

       statement = '''samtools flagstat %(infile)s > %(outfile)s'''

       P.run(statement)


Running the code again should generate the same output. However, if you
had bam files that came from a different genome build then the parameter in the yml file
can be modified easily, the output files deleted and the pipeline ran using the new configuration values.
