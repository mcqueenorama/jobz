jobz
====

zeromq queue based distributed job execution and synchronization behind mongrel2

The general idea is to have jobs for any pipeline to be executed
by workers subscribing to a queue.  Each job tries to do its task
and then publishes the call for the next step.  This allows a chain
of disconnected servers to participate in a sychronized sequence.
Work is pulled in from the publishing broker, so workers can be
behind a firewall and they can still pull in work requests.  The
broker must be accessible to all and the http frontend.

The implementation philosophy is to keep the code very simple, and
general. Its just a few simple python scripts, and a mongrel2 server.
The complexity is moved out of the code and into deploying and
managing the various workers. A good tool such as Ansible makes
that job pretty easy.  Everywhere a job will run, there will be a
worker listening to the queue.  The worker will simple execute some
local command, which may be a complex shell command.

All job data, such as its command string, is in yaml config files.
Each job loads a specified job db which contains the arbitrary
command to execute, and other data, and the command string includes
printf style format strings, so query string data from the URL can
be used to pass parameters to the job. Dangerous escape characters
are removed from the query string before being interpolated into
the command string.

The frontend is mongrel2.  The broker is connected to the web
frontend.  The workers all pull from the broker.  All work requests
are very simple and can be triggered from simple commands at the
command line like:

      curl -v http://whatever.com:6769/job_start?job_name=import_category_dump\&date=`/bin/date "+%Y_%m_%d"`

That command would cause all workers which are subscribed to the
topic to execute their configured commands, providing a date which
can be used in the command string with python style format strings.
When done these workers call the next step or they call a failure
handler.  Each job has an on success and an on failure handler.
Each job can also make any other calls by using the http interfact
to update status or trigger jobs.

Complex dependencies such as making one job depend on the successful
completion of any number of other jobs, so one job can run after
10 jobs finish the first 4 steps, or one job can trigger many other
jobs.  There's a simple semaphore worker which is used to handle
the complex dependencies.  When several instances of the semaphore
script are used in parallel, then act like a lock which is released
when the many dependents are done.  So this too is a complex problem,
solved with very simple code.  The semaphores need to be deployed
and started in the right way to handle the complex dependency, but
the code is very simple.

I have plans for securing it.  I'll make the mongrel2 frontend
accept requests only from my own certificate authority.

