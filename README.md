IMPORTANT NOTE: 

Our system uses marginally more Zookeeper connections, as such, 
all tests need to be run with the following flag set in the Zookeeper
configuration file.

maxClientCnxns=0

--- 

Job Tracker: 
start_jobtracker.sh <zookeeper host> <zookeeper port>

Worker:
start_worker.sh <zookeeper host> <zookeeper port>

FileServer:
start_fileserver.sh <zookeeper host> <zookeeper port>

Client 
submit_job.sh <zookeeper host> <zookeeper port> <password hash>
— this allows a user to submit a job request. It prints the job id to the console, the student can defined their own student ID

--- 

The order in which services begin do not matter, each waits for the services they communicate with or depend on before beginning. 

Detailed design information is available in design.pdf.


