import sys
import os
import re
import setuptools
from setuptools import setup, find_packages, Extension

from distutils.version import LooseVersion
if LooseVersion(setuptools.__version__) < LooseVersion('1.1'):
    print("Version detected:", LooseVersion(setuptools.__version__))
    raise ImportError(
        "the CGAT code collection requires setuptools 1.1 higher")

########################################################################
########################################################################
IS_OSX = sys.platform == 'darwin'

########################################################################
########################################################################
# collect CGAT version
sys.path.insert(0, "cgatcore")
import version

version = version.__version__

###############################################################
###############################################################
# Define dependencies
#
major, minor1, minor2, s, tmp = sys.version_info

if major < 3:
    raise SystemExit("""CGAT requires Python 3 or later.""")

cgat_packages = find_packages()
cgat_package_dirs = {'cgatcore': 'cgatcore'}

##########################################################
##########################################################
# Classifiers
classifiers = """
Development Status :: 3 - Alpha
Intended Audience :: Science/Research
Intended Audience :: Developers
License :: OSI Approved
Programming Language :: Python
Topic :: Software Development
Topic :: Scientific/Engineering
Operating System :: POSIX
Operating System :: Unix
Operating System :: MacOS
"""

setup(
    # package information
    name='cgatcore',
    version=version,
    description='cgatcore : the Computational Genomics Analysis Toolkit',
    author='Andreas Heger',
    author_email='andreas.heger@gmail.com',
    license="MIT",
    platforms=["any"],
    keywords="computational genomics",
    long_description='CGAT : the Computational Genomics Analysis Toolkit',
    classifiers=[_f for _f in classifiers.split("\n") if _f],
    url="https://github.com/cgat-developers/cgat-core",
    # package contents
    packages=cgat_packages,
    package_dir=cgat_package_dirs,
    include_package_data=True,
    # other options
    zip_safe=False,
    test_suite="tests",
)
