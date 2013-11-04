#!/usr/bin/env python

import os
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
import worker
from worker import query_string_dict
from worker import HTTP_ENDPOINT
import json 
import uuid

parser = argparse.ArgumentParser(description='send a job on to a worker')
parser.add_argument('--pid',
                   default="./run/%s.pid" % os.path.basename(__file__),
                   help='the pid to check for the check process. the running check needs a pid of its own for process management')
parser.add_argument('--recv_url',
                   default="tcp://" + HTTP_ENDPOINT + ":6733",
                   help='the url for the recv queue')
parser.add_argument('--send_url',
                   default="tcp://" + HTTP_ENDPOINT + ":6734",
                   help='the url for the sending queue')
parser.add_argument('--status_url',
                   default="http://" + HTTP_ENDPOINT + ":6769/catalog/status",
                   help='the url for setting the status of the current job ')

args = parser.parse_args()

pidfile = args.pid
send_url = args.send_url
recv_url = args.recv_url
status_url = args.status_url

today=date.today()
date= today.strftime('%Y-%m-%d')

syslog.syslog("pid:%s:starting to watch queues to start jobs:pidfile:%s:send_url:%s:recv_url:%s:status_url:%s:" % ( os.getpid(), pidfile, send_url, recv_url, status_url))

print "starting to watch queues to start jobs:pidfile:%s:send_url:%s:recv_url:%s:status_url:%s:" % ( pidfile, send_url, recv_url, status_url)

def signal_handler(signal, frame):
        print "caught signal. removing pid..."
        os.path.isfile(pidfile) and os.unlink(pidfile)
        sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGABRT, signal_handler)

pid = str(os.getpid())

with open(pidfile, 'w') as f:
            pid = str(os.getpid())
            f.write(pid)

sender_id = uuid.uuid4().hex 

conn = handler.Connection(sender_id, send_url, recv_url) 
worker = worker.Worker(sender_id, recv_url) 

while True: 

      print "WAITING FOR REQUEST" 
      req = conn.recv() 
      response = "";
   
      if req.is_disconnect(): 
          print "DISCONNECT" 
          continue 

      #a globally unique job id 
      job_id=str(uuid.uuid1())

      job_name = ""
      qsd=query_string_dict(req.headers)

      conn.reply_http(req, response)
      if not qsd:
        print "malformed query string:rejecting request:headers:%s:" % json.dumps(req.headers, encoding='ascii')
        continue

      if not 'job_name' in qsd:
        print "no jobname found in query string:rejecting request:headers:%s:" % json.dumps(req.headers, encoding='ascii')

      else:
        job_name = str(qsd['job_name'])
        print "job_name=:%s:job_id:%s:" % (job_name, job_id)

        (rv, status_msg) = worker.send_request(job_name, {'job_id': job_id, 'sender': req.sender, 'path': req.path, 'conn_id': req.conn_id, 'headers': json.dumps(req.headers, encoding='ascii'), 'body': req.body})
        print "done sending work request"

os.unlink(pidfile)
