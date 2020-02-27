## FStream

### Application level throughput optimization. 
Master Thesis Project: High-performance data transfer optimization via dynamic settings. Developed a system that performs application-
level changes on the network to achieve high and stable throughput during file transfers. The system adapts pipelining,
parallelism, and concurrency parameters to file chunks characteristics like size and file count. Initial results show that the
algorithm lowers network overhead about 30% - 40% with at most 10% throughput decrease. The project developed as a
Java multithreading application. Also, I use XSede GridFTP and JGlobus API, and supercomputers.



### Project structure
```
.AdaptiveGridFTPClient/
├── src                              # Source files 
|    ├── main                
|    |    ├── shell-scripts            # helper scritps
|    |    ├── java                     # java codes
|    |    ├── python                   # Experiments and optimizer
|    |    ├── resources                # configuration files
|    ├── test                         # tests
├── logs                             # transfer logs
└── README.md
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

