from worker import report_job_status
import unittest
import platform
import urllib
from datetime import datetime

class TestWorkerFunctions(unittest.TestCase):

    def test_report_status_null(self):
        self.assertFalse(report_job_status(""))

    def test_report_status_msg(self):
        self.assertTrue(report_job_status("msg1"))

    def test_report_status_qsd(self):
        qsd={'job_name': 'test_job', 'job_id': 'test_job_id', 'sender': 'test_sender', 'date': 'test_date'}
        self.assertTrue(report_job_status("msg2", qsd))

    def test_report_status_example(self):
        job_name='test_job11'
        command='test command 22'
        self.assertTrue(report_job_status("worker started:command:" + command + ":" + str(datetime.now()) + ":", {'job_name': job_name, 'job_id': None, 'sender': None, 'date': 'test_date'}))

    def test_report_status_example_date(self):
        job_name='test_job11'
        command='test command 22'
        self.assertTrue( report_job_status("worker started:node:" + platform.node() + ":command:" + command + ":", {'job_name': job_name, 'date': str(datetime.now()) }))

    def test_report_status_example_no_date(self):
        job_name='test_job11'
        command='test command 22'
        self.assertTrue( report_job_status("worker started:node:" + platform.node() + ":command:" + command + ":", {'job_name': job_name }))

    def test_report_status_example_request(self):
        job_name='test_job11'
        command='test command 22'
        request='725:4:body,0:,6:job_id,36:f1d983ea-400f-11e3-b2ed-00163e13ed1b,8:pub_addr,38:tcp://' + HTTP_ENDPOINT + ':6733,9:sender_id,32:03d38fede76a4420b4675f503d936294,7:conn_id,6:349802,7:headers,460:{"PATTERN": "/test_start/", "x-forwarded-for": "172.28.208.202", "URI": "/test_start?job_name=bfd_sync_import_hierarchy_pch_ids&date=2013_10_28", "accept": "*/*", "user-agent": "curl/7.19.7 (x86_64-redhat-linux-gnu) libcurl/7.19.7 NSS/3.14.3.0 zlib/1.2.3 libidn/1.18 libssh2/1.4.2", "host":' + HTTP_ENDPOINT + '":6769", "VERSION": "HTTP/1.1", "QUERY": "job_name=bfd_sync_import_hierarchy_pch_ids&date=2013_10_28", "PATH": "/test_start", "METHOD": "GET"},4:path,11:/test_start,6:sender,36:4690C3EC-AEA7-4619-BBDB-EF4D84F832AC,}'
        self.assertTrue( report_job_status("I:worker got some work:node:" + platform.node() + ":request:" + request + ":", {'job_name': job_name, 'date': str(datetime.now()) } ))

if __name__ == '__main__':
    unittest.main()
