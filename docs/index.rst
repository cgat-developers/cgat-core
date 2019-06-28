.. _manual-main:

========================
CGAT-core documentation!
========================

.. image:: https://img.shields.io/github/license/cgat-developers/cgat-core.svg
    :alt: Licence

.. image:: https://img.shields.io/conda/v/bioconda/cgatcore.svg
    :alt: Conda

.. image:: https://readthedocs.org/projects/cgat-core/badge/?version=latest
    :target: http://cgat-core.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://img.shields.io/travis/cgat-developers/cgat-core.svg
    :alt: Travis

.. image:: https://img.shields.io/twitter/follow/CGAT_Oxford.svg?style=social&logo=twitter&label=Follow
    :target: https://twitter.com/cgat_oxford?lang=en
    :alt: Twitter Followers

.. image:: https://img.shields.io/twitter/url/http/shields.io.svg?style=social&logo=twitter
    :target: https://twitter.com/cgat_oxford?lang=en
    :alt: Twitter URL


CGAT-core is a workflow management system that allows users to quickly and reproducibly build scalable
data analysis pipelines. CGAT-core is  a set of libraries and helper functions used to enable researchers
to design and build computational workflows for the analysis of large-scale data-analysis.

Used in combination with `CGAT-apps <https://github.com/cgat-developers/cgat-apps>`_, we have deomonstrated the functionality of our
flexible workflow management system using a simple RNA-seq pipeline in `cgat-showcase <https://github.com/cgat-developers/cgat-showcase>`_.

CGAT-core is open-sourced, powerful and user-friendly, and has been continually developed
as a Next Generation Sequencing (NGS) workflow management system over the past 10 years.

For more advanced examples of cgatcore utilities please refer to our `cgat-flow <https://github.com/cgat-developers/cgat-flow>`_ repository, however
please be aware that this is in constant development and has many software dependancies.


.. _manual-quick_example:

--------
Citation
--------

To be added....

.. _manual-support:

-------
Support
-------

- Please refer to our :ref:`FAQ` section
- For bugs and issues, please raise an issue on `github <https://github.com/cgat-developers/cgat-core>`_
- For contributions, please refer to our contributor section and `github <https://github.com/cgat-developers/cgat-core>`_ source code.

--------
Examples
--------

**cgat-showcase**
   This is a toy example of how to develop a simple workflow. Please refer to the `github page <https://github.com/cgat-developers/cgat-showcase>`_ and the `documentation <https://cgat-showcase.readthedocs.io/en/latest/>`_.
**cgat-flow**
   As an example of the flexibility and functionality of CGAT-core, we have developed a set of fully tested production pipelines for automating the analysis of our NGS data. Please refer to the `github <https://github.com/cgat-developers/cgat-flow>`_ page for information on how to install and use our code.
**Single cell RNA-seq**
   The cribbs lab use CGAT-core to develop pseudoalignment pipelines for single cell `dropseq methods <https://github.com/Acribbs/single-cell>`_ 
   The sansom lab use the CGAT-core workflow engine to develop single cell `sequencing analysis workflows <https://github.com/sansomlab/tenx>`_.


-------------------------------------
Selected publications using CGAT-core
-------------------------------------

CGAT-core has been developed over the past 10 years and as such has been used in many previously published articles

For a non-comprehensive list of citations please see our :citing and :ref:`project_info-citations`




.. toctree::
   :caption: Getting started
   :name: getting_started
   :maxdepth: 1
   :hidden:

   getting_started/Installation.rst
   getting_started/Cluster_config.rst
   getting_started/Examples.rst
   getting_started/Tutorial.rst

.. toctree::
   :caption: Build a workflow
   :name: build
   :maxdepth: 1
   :hidden:

   defining_workflow/Writing_workflow.rst
   defining_workflow/Tutorial.rst

.. toctree::
   :caption: Working with remote files
   :name: build
   :maxdepth: 1
   :hidden:

   remote/S3.rst
   remote/GC.rst
   remote/Azure.rst

.. toctree::
   :caption: cgatcore functions
   :name: function_doc
   :maxdepth: 1
   :hidden:

   function_doc/Pipeline.rst
   function_doc/Core.rst

.. toctree::
   :caption: Project Info
   :name: project-info
   :maxdepth: 1
   :hidden:

   project_info/Contributing.rst
   project_info/how_to_contribute.rst
   project_info/citations.rst
   project_info/FAQ.rst
   project_info/Licence.rst
