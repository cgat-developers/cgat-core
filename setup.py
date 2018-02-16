import sys
import os
import subprocess

########################################################################
########################################################################
# Import setuptools
# Use existing setuptools, otherwise try ez_setup.
try:
    import setuptools
except ImportError:
    # try to get via ez_setup
    # ez_setup did not work on all machines tested as
    # it uses curl with https protocol, which is not
    # enabled in ScientificLinux
    import ez_setup
    ez_setup.use_setuptools()

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
sys.path.insert(0, "CGATCore")
import version

version = version.__version__

###############################################################
###############################################################
# Define dependencies
#
major, minor1, minor2, s, tmp = sys.version_info

if (major == 3 and minor1 < 6) or major < 3:
    raise SystemExit("""CGATCore requires Python 3.6 or later.""")

#####################################################################
#####################################################################
# Dependencies are now managed with conda
#####################################################################

install_requires = []
dependency_links = []

cgat_packages = find_packages()
cgat_package_dirs = {'CGATCore': 'CGATCore'}

##########################################################
##########################################################
# Classifiers
classifiers = """
Development Status :: Beta
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
    name='CGATCore',
    version=version,
    description='CGAT Core',
    author='cgat-developers',
    author_email='andreas.heger@gmail.com',
    license="MIT",
    platforms=["any"],
    keywords="computational genomics",
    long_description='Core for the Computational Genomics Analysis Toolkit',
    classifiers=[_f for _f in classifiers.split("\n") if _f],
    url="https://github.com/cgat-developers/cgat-core",
    # package contents
    packages=cgat_packages,
    package_dir=cgat_package_dirs,
    include_package_data=True,
#    entry_points={
#        'console_scripts': ['cgat = CGAT.cgat:main']
#    },
    # dependencies
    install_requires=install_requires,
    dependency_links=dependency_links,
    # other options
    zip_safe=False,
    test_suite="tests",
)
