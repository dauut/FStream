## FStream - Stream Data Transfer Optimization for Distributed Scientific Workflows

#### Real-time Application level throughput optimization. 

Robust   and   predictable   network   performance   iscritical for distributed science workflows to move large volumesof  streaming  data  and  help  breakthrough  discoveries  be  madetimely. However, existing data transfer applications are designedfor  batch  workloads  in  a  way  that  their  settings  cannot  bealtered once they are set. This, in turn, severely limits streamingapplications  from  adapting  to  changing  dataset  and  networkconditions to meet stringent transfer performance requirements.In  this  paper,  we  proposeFStreamto  offer  performance  guar-antees  to  time-sensitive  streaming  applications  by  continuouslymonitoring transfer performance and adjusting transfer settingsthrough online profiling to adapt dynamic system conditions andsustain high network performance.FStreamalso takes advantageof  long-running  nature  of  streaming  workflows  and  keeps  trackof  past  profiling  results  to  greatly  reduce  convergence  time  offuture online profiling executions. We evaluated the performanceofFStreamby   transferring   several   synthetic   and   real-worldworkloads   using   high-performance   production   networks   andfound  that  it  offers  up  to  an  order  of  magnitude  performanceimprovement  over  state-of-the-art  transfer  applications.

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
### Reqiuriments
- Linux environment. 
- This project runs on XSEDE Grid computer networks. Also, it uses `myproxy-logon` certificate.<br>
```myproxy-logon -s myproxy.xsede.org -l [username] -t 9999```

### Parameters
```
-s				: source path
-d				: destination path
-rtt				: round trip time 
-bandwidth [value]		: user defined bw value
-profiling			: activate Profiling transfer
-static				: activate Static transfer
-qos				: activate Quality of Service
-speedLimit [value]		: set QoS speed limit
-buffer-size
```

#### Evaluation
In this project, we used the [GridFTP](https://en.wikipedia.org/wiki/GridFTP) and [JGlobus](https://github.com/jglobus/JGlobus/) to transfer the huge volume of data between three pairs of [XSEDE](https://www.xsede.org/) sites. Thus, we increased average throughput by 2.3x-9.1x comparatively to previous works and existing transfer methods.  

<p align="center">
  <img width="500" height="375" src="https://raw.githubusercontent.com/dauut/FStream/master/imgs/st2_comet_results.png">
</p>

Our proposed model adapts transfer settings according to ongoing throughput rates and changing file characteristics during transfers. Thus, it changes concurrency, pipelining and parallelism value according to the heuristic algorithm. Results showed that profiling transfers outperform other existing models. 

<p align="center">
  <img width="720" height="365" src="https://github.com/dauut/FStream/blob/master/imgs/inst-throughput.png?raw=true">
</p>
