## FStream

#### Application level throughput optimization. 

FStream offers performance guarantees to time-sensitive streaming applications 
by continuously monitoring transfer performance and adjusting transfer settings through online profiling
to adapt dynamic system conditions and sustain high network performance. 
FStream also takes advantage of long-running nature of streaming workflows and keeps track of 
past profiling results to greatly reduce
convergence time of future online profiling executions. We evaluated the
performance of FStream by transferring several synthetic and real-world
workloads using high-performance production networks and found that
it offers up to an order of magnitude performance improvement over
state-of-the-art transfer applications.





#### Project structure
```
.AdaptiveGridFTPClient/             # FStream Client
├── src                              # Source files 
|    ├── main                
|    |    ├── shell-scripts            # helper scritps
|    |    ├── java                     # java codes
|    |    ├── python                   # Experiments and optimizer
|    |    ├── resources                # configuration files
|    ├── test                         # tests
├── logs                             # transfer logs
└── README.md
.axis/                              #lib
.gridftp/                           #lib
...                                 #lib
```

## Installation

### Requirements
* Linux environment
* Java 1.8+
* python 2.7+
* Maven

### Usage 

#### Configuration

Edit configurations in config.cfg in src/main/resources folder as follows <br>
-s $Source_GridFTP_Server <br>
-d $Destination_GridFTP_Server <br>
-proxy $Proxy_file_path (Default will try to read from /tmp for running user id) <br>
-cc $maximum_allowed_concurrency <br>
-rtt $rtt (round trip time between source and destination) in ms <br>
-bw $bw (Maximum bandwidth between source and destination) in Gbps <br>
-bs $buffer_size (TCP buffer size of minimum of source's read and destination's write in MB)<br>
[-single-chunk] (Will use Single Chunk SC approach to schedule transfer. Will transfer one chunk at a time)<br>
[-useHysterisis] (Will use historical data to run modelling and estimate transfer parameters. [HARP]. Requires python to be installed with scipy and sklearn packages)<br>
[-use-dynamic-scheduling] (Provides dynamic channel reallocation between chunks while transfer is running ProMC)<br>
Sample config file can be found in main/src/resources/confif.cfg<br>
#### create_files.sh 
This \script creates memory dumps as files with desired parameters. Files will create as FILENAME + Index Number. <br>
`. ./create_files.sh [destination directory] [file name] [start index] [end index] [random seed] [dump size]`

