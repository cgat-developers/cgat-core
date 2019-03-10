"""Unit tests for cgatcore.pipeline.cluster.py"""

import unittest
import os
import shutil
import tempfile
import time
import collections
import cgatcore.pipeline.cluster as cluster


class TestSlurmValueParsing(unittest.TestCase):

    def check(self, data, expected):
        resource_usage = cluster.JobInfo(1, {})
        c = cluster.SlurmCluster.parse_accounting_data(data, resource_usage)
        self.assertEqual(c[0].resourceUsage, expected)

    def test_parsing_short_job(self):

        sacct = ["host1|267088|2019-02-27T00:39:42|2019-02-27T02:58:15|"
                 "2019-02-27T05:05:32|1|0:0|7637|7637|01:54:55|01:11.853|||||||||0:0|",
                 "host1|267088.batch|2019-02-27T02:58:15|2019-02-27T02:58:15|"
                 "2019-02-27T05:05:32|1|0:0|7637|7637|01:54:55|01:11.853|"
                 "0.10M|0.00M|10060K|9292520K|12087040K|150280K|77K|9292520K||77K"]
        self.check(sacct[-1],
                   {'NodeList': 'host1',
                    'JobID': '267088.batch',
                    'Submit': 1551236295,
                    'Start': 1551236295,
                    'End': 1551243932,
                    'NCPUS': 1,
                    'ExitCode': 0,
                    'ElapsedRaw': 7637,
                    'CPUTimeRaw': 7637,
                    'UserCPU': 6895,
                    'SystemCPU': 71,
                    'MaxDiskRead': 100000,
                    'MaxDiskWrite': 0,
                    'AveVMSize': 10060000,
                    'AveRSS': 9292520000,
                    'MaxRSS': 12087040000,
                    'MaxVMSize': 150280000,
                    'AvePages': 77000,
                    'DerivedExitCode': '',
                    'MaxPages': 77000})

    def test_parsing_longer_than_24h_job(self):

        sacct = ["host2|267087|2019-02-27T00:39:42|2019-02-27T02:58:08|"
                 "2019-02-28T04:38:50|1|0:0|92442|92442|1-01:12:52|19:36.307|||||||||0:0|",
                 "host2|267087.batch|2019-02-27T02:58:08|2019-02-27T02:58:08|"
                 "2019-02-28T04:38:50|1|0:0|92442|92442|1-01:12:52|19:36.307|"
                 "0.10M|0.00M|10060K|26253156K|33016300K|150280K|580K|26253156K||580K"]
        self.check(sacct[-1],
                   {'NodeList': 'host2',
                    'JobID': '267087.batch',
                    'Submit': 1551236288,
                    'Start': 1551236288,
                    'End': 1551328730,
                    'NCPUS': 1,
                    'ExitCode': 0,
                    'ElapsedRaw': 92442,
                    'CPUTimeRaw': 92442,
                    'UserCPU': 90772,
                    'SystemCPU': 1176,
                    'MaxDiskRead': 100000,
                    'MaxDiskWrite': 0,
                    'AveVMSize': 10060000,
                    'AveRSS': 26253156000,
                    'MaxRSS': 33016300000,
                    'MaxVMSize': 150280000,
                    'AvePages': 580000,
                    'DerivedExitCode': '',
                    'MaxPages': 580000})
