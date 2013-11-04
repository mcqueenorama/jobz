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
import platform
from datetime import datetime

import mongrel2.tnetstrings 

from worker import WORKER_ENDPOINT
from worker import HTTP_ENDPOINT
from worker import get_job_data
from worker import report_job_status
from worker import query_string_dict

parser = argparse.ArgumentParser(description='run a job in the backend somewhere.  This thing subscribes to a job publisher.')
parser.add_argument('--pid',
                   default="./run/%s.pid" % os.path.basename(__file__),
                   help='the pid to check for the check process. the running check needs a pid of its own for process management')
parser.add_argument('--jobname',
                   required=True,
                   help='the job name which is to be pulled from the job piblisher queue')
parser.add_argument('--jobdb',
                   default='~/conf/worker/jobs.yml',
                   help='the yaml file which has all the job info')
parser.add_argument('--listname',
                   default='sqoop_jobs',
                   help='the name of the list in the jobdb')
parser.add_argument('--status',dest='status',action='store_true',help='do status reports')
parser.add_argument('--no-status',dest='status',action='store_false',help='don\'t status reports')
parser.set_defaults(status=True)

args = parser.parse_args()

pidfile = args.pid
job_name = args.jobname
job_db = args.jobdb
list_name = args.listname

job_info = get_job_data(job_db, job_name, list_name)

if not job_info:
  print "I:worker:job_name:%s:no job info for job_name:%s:ignoring it ..." % (job_name, job_name)
  sys.exit()

print "I:worker:job_name:%s:found job info for job_name:%s:" % (job_name, job_name)
command=job_info['command']
print "I:worker:job_name:%s:ready to do work:command:%s:" %  (job_name, command)

on_success=job_info['on_success'] if job_info['on_success'] else None
on_failure=job_info['on_failure'] if job_info['on_failure'] else None
print "I:worker:job_name:%s:next steps:on_success:%s:on_failure:%s:" %  (job_name, on_success, on_failure)

def make_command_str(command, job_name, job_id, qsd):

  try:
    exec_command = command.format(**qsd) 
  except TypeError:
    print "ERROR:bad args:worker:job_name:{0}:job_id:{1}:command:{2}:query string:{3}:using bare command:".format(job_name, job_id, command, str(qsd))
    exec_command = command
  except KeyError:
    print "ERROR:bad args:worker:job_name:{0}:job_id:{1}:command:{2}:query string:{3}:using bare command:".format(job_name, job_id, command, str(qsd))
    exec_command = command

  return exec_command

CTX = zmq.Context()
server = CTX.socket(zmq.SUB)
print "I:worker:job_name:%s:connecting to WORKER_ENDPOINT:%s:with topic:job_name:%s" % (job_name, WORKER_ENDPOINT, job_name)
args.status and report_job_status("worker started:node:" + platform.node() + ":command:" + command + ":", {'job_name': job_name })

#topicfilter = "10001"
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

cycles = 0
while True:

    cycles += 1

    print "I:worker:job_name:%s:waiting..." % job_name
    request = server.recv()
    print "I:worker:job_name:%s:got some work:%s:" % (job_name, request)
    args.status and report_job_status("I:worker got some work:node:" + platform.node() + ":request:" + request + ":", {'job_name': job_name } )
    now=str(datetime.now()) 

    (job_name, extra) = request.split(" ", 1)
    print "I:worker:job_name:%s:received:tnetstring:%s:" % (job_name, request)
    parsed_request, extra = mongrel2.tnetstrings.parse(extra)
    headers=json.loads(parsed_request['headers'])
    job_id = parsed_request['job_id'] if 'job_id' in parsed_request else None

    qsd=query_string_dict(headers)

    #its cleaner code if qsd is never null
    qsd['job_name'] = job_name
    qsd['job_id'] = job_id
    qsd['now'] = now
    qsd['sender'] = parsed_request['sender'] if 'sender' in parsed_request else None

    try:

        exec_command = command

        try:

          print("I:worker:job_name:%s:pid:%s:job_id:%s:processing args:%s:" % (job_name, os.getpid(), job_id, str(qsd)))
          if 'arg1' in qsd:
            exec_command = "%s %s" % (command, qsd['arg1'])
          else:
            exec_command = command.format(**qsd) 

        except:
          raise

        #command strings can work like this
        #>>> command="arga:{a}:argd:{d}:"
        #>>> args="a=2:d=3"
        #>>> args={'a': 1, 'd': 2}
        #>>> command.format(**args)
        #'arga:1:argd:2:'

        print("I:worker:job_name:%s:pid:%s:job_id:%s:starting job:exec_command:%s:qsd:%s:" % (job_name, os.getpid(), job_id, exec_command, str(qsd)))
        args.status and report_job_status("I:starting work:node:" + platform.node() + ":", qsd )
        ret_val =  commands.getstatusoutput(exec_command)
        print "I:worker:job_name:%s:done with job:job_id:%s:status:%d:output:%s:" % (job_name, job_id, ret_val[0], ret_val[1])

        args.status and report_job_status("I:done working:node:" + platform.node() + ":", qsd )
        if ret_val[0]:

          exec_on_failure =  make_command_str(on_failure, job_name, job_id, qsd)

          print "I:worker:job_name:%s:job_id:%s:calling failure handler:exec_on_failure:%s:" % (job_name, job_id, exec_on_failure)
          args.status and report_job_status("I:calling failure:node:" + platform.node() + ":", qsd )
          ret_val =  commands.getstatusoutput(exec_on_failure)

        else:

          exec_on_success =  make_command_str(on_success, job_name, job_id, qsd)

          print "I:worker:job_name:%s:job_id:%s:calling success handler:exec_on_success:%s:" % (job_name, job_id, exec_on_success)
          args.status and report_job_status("I:calling success:node:" + platform.node() + ":", qsd )
          ret_val =  commands.getstatusoutput(exec_on_success)

        print "called next step:job_id:%s:status:%d:output:%s:" % (job_id, ret_val[0], ret_val[1])

        # Simulate various problems, after a few cycles
        #if cycles > 3 and randint(0, 3) == 0:
            #print "I:worker:job_name:%s:Simulating a crash" % job_name
            #break
        #elif cycles > 3 and randint(0, 3) == 0:
            #print "I:worker:job_name:%s:Simulating CPU overload" % job_name
            #time.sleep(33)

    except OSError as e:
        print "ERROR:worker:job_name:{0}:job_id:{1}:error({2}):{3}:".format(job_name, job_id, e.errno, e.strerror)
    except TypeError:
        print "ERROR:bad args:worker:job_name:{0}:job_id:{1}:query string:{2}:".format(job_name, job_id, str(qsd))
    except KeyError:
        print "ERROR:bad args:worker:job_name:{0}:job_id:{1}:query string:{2}:".format(job_name, job_id, str(qsd))


server.close()
CTX.term()
os.unlink(pidfile)
