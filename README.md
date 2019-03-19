
![CGAT-core](https://github.com/cgat-developers/cgat-core/blob/master/docs/img/CGAT_logo.png)
----------------------------------------

<p align="left">
	<a href="https://readthedocs.org/projects/cgat-core/badge/?version=latest", alt="Documentation">
		<img src="https://readthedocs.org/projects/cgat-core/badge/?version=latest" /></a>
	<a href="https://travis-ci.org/cgat-developers/cgat-core", alt="Travis">
		<img src="https://img.shields.io/travis/cgat-developers/cgat-core.svg" /></a>
	<a href="https://twitter.com/cgat_oxford?lang=en", alt="Twitter followers">
		<img src="https://img.shields.io/twitter/url/http/shields.io.svg?style=social&logo=twitter" /></a>
	<a href="https://twitter.com/cgat_oxford?lang=en", alt="Twitter followers">
		<img src="https://img.shields.io/twitter/url/http/shields.io.svg?style=social&logo=twitter" /></a>
</p>

----------------------------------------

CGAT-core is a workflow management system that allows users to quickly and reproducibly build scalable
data analysis pipelines. CGAT-core is a set of libraries and helper functions used to enable researchers
to design and build computational workflows for the analysis of large-scale data-analysis. 

Documentation for CGAT-core can be accessed at [read the docs](http://cgat-core.readthedocs.io/en/latest/) 

Used in combination with CGAT-apps, we have demonstrated the functionality of our
flexible implementation using a set of well documented, easy to install and easy to use workflows, 
called [CGAT-flow](https://github.com/cgat-developers/cgat-flow) ([Documentation](https://www.cgat.org/downloads/public/cgatpipelines/documentation/)).

CGAT-core is open-sourced, powerful and user-friendly, and has been continually developed 
as a Next Generation Sequencing (NGS) workflow management system over the past 10 years.


Installation
============

The following sections describe how to install the [cgatcore](https://cgat-core.readthedocs.io/en/latest/index.html) framework. For instructions on how to install
our other repos, CGAT-apps (scripts) and CGAT-flow (workflows/pipelines), please follow these instructions [here](https://www.cgat.org/downloads/public/cgatpipelines/documentation/InstallingPipelines.html).

The preferred method to install the cgatcore is using conda, by following the instructions on [read the docs](https://cgat-core.readthedocs.io/en/latest/getting_started/Installation.html). However, there are a few other methods to install cgatcore, including pip and our own bash script installer.

Linux vs OS X
=============

* ulimit works as expected in Linux but it does not have an effect on OS X. [Disabled](https://github.com/cgat-developers/cgat-core/commit/d4d9b9fb75525873b291028a622aac70c44a5065) ulimit tests for OS X.

* ssh.connect times out in OSX. Exception [caught](https://github.com/cgat-developers/cgat-core/commit/d4d9b9fb75525873b291028a622aac70c44a5065)

* Linux uses /proc/meminfo and OS X uses [vm_stat](https://github.com/cgat-developers/cgat-core/compare/bb1c75df8f42...575f0699b326)

* Currently our testing framework is broken for OSX, however we are working to fix this. However, we dont envisage any issues running the code at present.

