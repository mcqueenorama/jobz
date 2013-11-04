from random import randint
import os
import time
import zmq
from uuid import uuid4 
import argparse
import signal
import syslog
import sys
import json
import commands

import mongrel2.tnetstrings 

from worker import WORKER_ENDPOINT
from worker import HTTP_ENDPOINT

parser = argparse.ArgumentParser(description='wait for a msg in the queue then die when it arrives')
parser.add_argument('--pid',
                   default="./run/%s.pid" % os.path.basename(__file__),
                   help='the pid to check for the check process. the running check needs a pid of its own for process management')
parser.add_argument('--jobname',
                   required=True,
                   help='the job name which is to be pulled from the job piblisher queue')

args = parser.parse_args()

pidfile = args.pid
job_name = args.jobname

CTX = zmq.Context()
server = CTX.socket(zmq.SUB)
print "I:semaphore:job_name:%s:connecting to WORKER_ENDPOINT:%s:with topic:job_name:%s" % (job_name, WORKER_ENDPOINT, job_name)
command='/usr/bin/curl -v http://' + HTTP_ENDPOINT + ':6769/status?job_name=%s\&status=locked' % job_name
commands.getstatusoutput(command)

server.setsockopt(zmq.SUBSCRIBE, job_name)
socket_id = uuid4().hex 
server.setsockopt(zmq.IDENTITY, socket_id)
server.connect(WORKER_ENDPOINT)

def signal_handler(signal, frame):
        print "caught signal. removing pid...:pidfile:%s:" % pidfile
        os.path.isfile(pidfile) and os.unlink(pidfile)
        sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGABRT, signal_handler)

pid = str(os.getpid())

with open(pidfile, 'w') as f:
            pid = str(os.getpid())
            f.write(pid)

print "I:semaphore:job_name:%s:waiting..." % job_name
request = server.recv()

print "I:semaphore:job_name:%s:got my signal:%s:shutting down..." % (job_name, request)
command='/usr/bin/curl -v http://' + HTTP_ENDPOINT + ':6769/job_status?job_name=%s\&status=unlocked' % job_name
commands.getstatusoutput(command)

server.close()
CTX.term()
os.unlink(pidfile)
