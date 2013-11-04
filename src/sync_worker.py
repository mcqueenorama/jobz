#!/usr/bin/env python

import os
import re
import sys
import socket
import syslog
import signal
import argparse
import time
from datetime import date
import commands
import json
import yaml

from mongrel2 import handler 
import mongrel2.tnetstrings 
from worker import load_job_db
from worker import load_job_info
from worker import HTTP_ENDPOINT

import json 
import uuid

parser = argparse.ArgumentParser(description='send a job on to a worker')
parser.add_argument('--pid',
                   default="./run/%s.pid" % os.path.basename(__file__),
                   help='the pid to check for the check process. the running check needs a pid of its own for process management')
parser.add_argument('--recv_url',
                   required=True,
                   help='the url for the recv queue')
parser.add_argument('--send_url',
                   required=True,
                   help='the url for the sending queue')
parser.add_argument('--status_url',
                   default="http://" + HTTP_ENDPOINT + ":6769/catalog/status",
                   help='the url for setting the status of the current job ')
parser.add_argument('--jobdb',
                   required=True,
                   help='the yaml file which has all the job info - the job db')
parser.add_argument('--jobname',
                   required=True,
                   help='the name of the job in the local yaml job db')
parser.add_argument('--listname',
                   required=True,
                   help='the name of the list in the jobdb')

args = parser.parse_args()

pidfile = args.pid
send_url = args.send_url
recv_url = args.recv_url
status_url = args.status_url
job_name = args.jobname

job_db = args.jobdb
listname = args.listname

jobs = load_job_db(job_db, listname)

job_info = load_job_info(jobs, job_name, listname)

if not job_info:
  print "I:worker:job_name:%s:no job info for job_name:%s:ignoring it ..." % (job_name, job_name)
  sys.exit()

print "I:worker:job_name:%s:found job info for job_name:%s:" % (job_name, job_name)
command=job_info['command']

today=date.today()
date= today.strftime('%Y-%m-%d')

print "I:worker:writing:pidfile:%s:" % (pidfile)
with open(pidfile, 'w') as f:
            pid = str(os.getpid())
            f.write(pid)

print "starting to watch queues to start jobs:pidfile:%s:send_url:%s:recv_url:%s:status_url:%s:command:%s:" % (pidfile, send_url, recv_url, status_url, command)

sender_id = str(uuid.uuid4())

conn = handler.Connection(sender_id, send_url, recv_url) 

safety_regex=re.compile('[^a-zA-Z0-9./:_-]')

while True: 
  print "WAITING FOR REQUEST" 
   
  req = conn.recv() 
   
  if req.is_disconnect(): 
      print "DISCONNECT" 
      continue 
   
  if req.headers.get("killme", None): 
      print "They want to be killed." 
      response = "" 

  else: 

      args = ""
      if 'QUERY' in req.headers:
        args = str(req.headers['QUERY'].split('=').pop())

      print "args=:%s:" % (args)

      #check that its ONLY \w or . or /
      args=safety_regex.sub("", args)

      try:

        exec_command = command % tuple(args.split(':'))

        ret_val =  commands.getstatusoutput(exec_command)
        print "I:worker:exec_command:%s:" % (exec_command)

        response = "<pre>\nSENDER: %r\nIDENT:%r\nPATH: %r\nHEADERS:%r\nBODY:%r:command:%s:rc:%s:output:%s:</pre>" % ( 
            req.sender, req.conn_id, req.path, 
            json.dumps(req.headers), req.body, command, ret_val[0], ret_val[1]) 
   
      except RuntimeError as e:
        response=e.strerror
      except TypeError as e:
        response="arguments wrong"

      print response 
   
  conn.reply_http(req, response)
