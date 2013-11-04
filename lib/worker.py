import zmq
import os
import commands
import time
import yaml
import urllib
from datetime import datetime
try:
    import json
except:
    import simplejson as json
from mongrel2.handler import Connection
from mongrel2.handler import CTX
import mongrel2.tnetstrings

REQUEST_TIMEOUT = 2500
REQUEST_RETRIES = 22
WORKER_ENDPOINT = "tcp://whatever:6795"
HTTP_ENDPOINT = "http://whatever:6769"

def get_job_data(sqoop_db, job_name, list_name='sqoop_jobs'):

  return load_job_info(load_job_db(sqoop_db, list_name), job_name, list_name)

def load_job_db(sqoop_db, list_name='sqoop_jobs'):

    if not sqoop_db:
      return None

    fhandle = file(sqoop_db, 'r')
    try:
        print "loading the yaml:sqoop_db:%s:" % sqoop_db
        jobs=yaml.load(fhandle)
        print "loaded sqoop_db:size:%d" % len(jobs[list_name])
        #for job in filter((lambda x: x['name'] == job_name), jobs['sqoop_jobs']):
            #command = job['cron']['command']
    finally:
        fhandle.close()

    return jobs

def report_job_status(qstatus_msg, qsd={}):

  now=str(datetime.now()) 
  calling_job='none'
  job_id='none'
  date='none'
  sender='none'
  job_name='job_status'

  if not qstatus_msg:
    print "report_job_status:somebody tried to do a status report with no args" 
    return False

  if qsd:
    calling_job = qsd['job_name'] if 'job_name' in qsd else 'none'
    job_id = qsd['job_id'] if 'job_id' in qsd else 'none'
    date = qsd['date'] if 'date' in qsd else 'none'
    sender = qsd['sender'] if 'sender' in qsd else 'none'

  qsd['now'] = now

  command="/usr/bin/curl -s '" + HTTP_ENDPOINT + "/test_start?job_name=job_status&date={date}&calling_job={calling_job}&job_id={job_id}&msg={qstatus_msg}&sender={sender}'"

  exec_qsd = {'date': date.replace(' ', '_'), 'calling_job': calling_job, 'job_id': job_id, 'qstatus_msg': urllib.quote_plus(qstatus_msg), 'sender': sender}
  try:

    #print("I:worker:job_name:%s:pid:%s:job_id:%s:processing args:%s:" % (job_name, os.getpid(), job_id, str(qsd)))
    exec_command = command.format(**exec_qsd) 

  except:
    print "report_job_status:bad args:command:%s:qstatus_msg:%s:exec_qsd:%s:" % (command, qstatus_msg, str(exec_qsd))
    return False

  print "report_job_status:job_name:%s:pid:%s:starting job:exec_command:%s:" % (job_name, os.getpid(), exec_command)
  ret_val =  commands.getstatusoutput(exec_command)
  print "report_job_status:job_name:%s:done with job:status:%d:output:%s:" % (job_name, ret_val[0], ret_val[1])
  return True

def load_job_info(jobs, job_name, list_name='sqoop_jobs'):

    if not jobs:
      return None

    if not jobs:
      return None

    print "scanning job db for job_name:%s:" % job_name
    for job in filter((lambda x: x['name'] == job_name), jobs[list_name]):
        print "loaded sqoop_db:job_name:%s" % job_name
        return job

    print "no job found for:job_name:%s:" % job_name

    return None

def query_string_dict(headers):

  qsd=dict()

  if 'QUERY' in headers:
    try:
      qsd=dict([ tuple(qarg.split('=')) for qarg in headers['QUERY'].split('&') ])
    except ValueError:
      print "malformed querystring:query string:%s:request:%s:" % (qsd, json.dumps(headers, encoding='ascii'))

  return qsd

class Worker(Connection):
    """
    This is a Connection with a REQ queue which is to be used by workers
    via a REP instead of a PUB back to the mongrel2
    each worker will need to do a PUB back to the mongrel2 queue

    """

    def __init__(self, sender_id, pub_addr):
        """
        Your addresses should be the same as what you configured
        in the config.sqlite for Mongrel2 and are usually like 
        tcp://127.0.0.1:9998
        """
        self.sender_id = sender_id

        req = CTX.socket(zmq.PUB)
        print "connecting to worker at:WORKER_ENDPOINT:%s:" % WORKER_ENDPOINT
        req.bind(WORKER_ENDPOINT)

        self.pub_addr = pub_addr
        self.req = req

    def send_request(self, job_name, request):

        request['pub_addr'] = self.pub_addr
        request['sender_id'] = self.sender_id

        work_msg="%s %s" % (job_name, mongrel2.tnetstrings.dump(request))

        print "I: Sending (%s)" % work_msg
        self.req.send(work_msg)

        print "I: sent it"
        return (0, "sent it")


    def cleanup(self):
        self.req.close()
        #CTX.term()


