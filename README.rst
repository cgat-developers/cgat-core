=========
cgat-core
=========

Core functionality of the CGAT code.

To-do documentation

Linux vs OS X
=============

* ulimit works as expected in Linux but it does not have an effect on OS X.
Disabled ulimit tests for OS X (https://github.com/cgat-developers/cgat-core/commit/d4d9b9fb75525873b291028a622aac70c44a5065)

* ssh.connect times out in OSX. Exception caught (https://github.com/cgat-developers/cgat-core/commit/d4d9b9fb75525873b291028a622aac70c44a5065)

* Linux uses /proc/meminfo and OS X uses vm_stat (https://github.com/cgat-developers/cgat-core/compare/bb1c75df8f42...575f0699b326)
