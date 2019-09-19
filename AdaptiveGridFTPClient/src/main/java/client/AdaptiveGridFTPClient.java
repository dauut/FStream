package client;

import client.hysterisis.Entry;
import client.hysterisis.Hysteresis;
import client.log.LogManager;
import client.utils.Utils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import transfer_protocol.module.GridFTPClient;
import transfer_protocol.util.XferList;
import java.util.ArrayList;

public class AdaptiveGridFTPClient {

  public static Entry transferTask;
  public static boolean isTransferCompleted = false;
  private GridFTPClient gridFTPClient;
  Hysteresis hysteresis;
  ConfigurationParams conf;
  private static final Log LOG = LogFactory.getLog(AdaptiveGridFTPClient.class);


  public AdaptiveGridFTPClient() {
    //initialize output streams for message logging
    conf = new ConfigurationParams();
    LogManager.createLogFile(ConfigurationParams.STDOUT_ID);
    LogManager.createLogFile(ConfigurationParams.INFO_LOG_ID);
  }

  @VisibleForTesting
  public AdaptiveGridFTPClient(GridFTPClient gridFTPClient) {
    this.gridFTPClient = gridFTPClient;
    LogManager.createLogFile(ConfigurationParams.STDOUT_ID);
  }

  public static void main(String[] args) throws Exception {
    AdaptiveGridFTPClient adaptiveGridFTPClient = new AdaptiveGridFTPClient();
    adaptiveGridFTPClient.parseArguments(args);
    adaptiveGridFTPClient.transfer();
  }

  private void parseArguments(String[] arguments) {
    conf.parseArguments(arguments, transferTask);

  }

  @VisibleForTesting
  void transfer() throws Exception {
    // Setup new transfer task based on user arguments
    transferTask = new Entry();
    transferTask.setSource(conf.source);
    transferTask.setDestination(conf.destination);
    transferTask.setBandwidth(conf.bandwidth);
    transferTask.setRtt(conf.rtt);
    transferTask.setBDP((transferTask.getBandwidth() * transferTask.getRtt()) / 8); // In MB
    transferTask.setBufferSize(conf.bufferSize);
    transferTask.setMaxConcurrency(conf.maxConcurrency);

    if (gridFTPClient == null) {
      gridFTPClient = new GridFTPClient(conf.source, conf.destination, conf.proxyFile);
      gridFTPClient.start();
      gridFTPClient.waitFor();
    }

    // create Control Channel to source and destination server
    double startTime = System.currentTimeMillis();

    if (gridFTPClient == null || GridFTPClient.ftpClient == null) {
      LOG.info("Could not establish GridFTP connection. Exiting...");
      System.exit(-1);
    }

    //Additional transfer configurations
    gridFTPClient.useDynamicScheduling = conf.useDynamicScheduling;
    gridFTPClient.useOnlineTuning = conf.useOnlineTuning;
    gridFTPClient.setPerfFreq(conf.perfFreq);
    GridFTPClient.ftpClient.setEnableIntegrityVerification(conf.enableIntegrityVerification);

    //First fetch the list of files to be transferred
    XferList dataset = gridFTPClient.getListofFiles();
    long datasetSize = dataset.size();
    LOG.info("file listing completed at:" + ((System.currentTimeMillis() - startTime) / 1000.0) +
            " data size:" + Utils.printSize(datasetSize, true));
    ArrayList<FileCluster> chunks = Utils.createFileClusters(dataset, conf.bandwidth, conf.rtt, conf.maximumChunks);
    if (conf.useHysterisis) {
      // Initialize historical data analysis
      hysteresis = new Hysteresis();
      hysteresis.findOptimalParameters(chunks, transferTask);
    }


    int[][] estimatedParamsForChunks = new int[chunks.size()][4];
    long timeSpent = 0;
    long start = System.currentTimeMillis();
    gridFTPClient.startTransferMonitor();
    switch (conf.algorithm) {
      case SINGLECHUNK:
        chunks.forEach(chunk->chunk.setTunableParameters(Utils.getBestParams(chunk.getRecords(), conf.maximumChunks)));
        if (conf.useMaxCC) {
          chunks.forEach(chunk -> chunk.getTunableParameters().setConcurrency(
              Math.min(transferTask.getMaxConcurrency(), chunk.getRecords().count())));
        }
        GridFTPClient.executor.submit(new GridFTPClient.ModellingThread());
        chunks.forEach(chunk -> gridFTPClient.runTransfer(chunk));
        break;
      default:
        // Make sure total channels count does not exceed total file count
        int totalChannelCount = Math.min(conf.maxConcurrency, dataset.count());
        if (conf.useHysterisis) {
          int maxConcurrency = 0;
          for (int i = 0; i < estimatedParamsForChunks.length; i++) {
            //fileClusters.get(i).getRecords().setTransferParameters(estimatedParamsForChunks[i]);
            if (estimatedParamsForChunks[i][0] > maxConcurrency) {
              maxConcurrency = estimatedParamsForChunks[i][0];
            }
          }
          totalChannelCount = maxConcurrency;
        } else {
          for (int i = 0; i < estimatedParamsForChunks.length; i++) {
            chunks.get(i).setTunableParameters(Utils.getBestParams(chunks.get(i).getRecords(), conf.maximumChunks));
          }
        }
        LOG.info(" Running MC with :" + totalChannelCount + " channels.");
        for (FileCluster chunk : chunks) {
          LOG.info("Chunk :" + chunk.getDensity().name() + " cc:" + chunk.getTunableParameters().getConcurrency() +
          " p:" + chunk.getTunableParameters().getParallelism() + " ppq:" + chunk.getTunableParameters().getPipelining());
        }
        Utils.allocateChannelsToChunks(chunks, totalChannelCount, conf.channelDistPolicy);
        for (FileCluster fileCluster: chunks) {
          gridFTPClient.runTransfer(fileCluster);
        }
        start = System.currentTimeMillis();
        //gridFTPClient.runMultiChunkTransfer(chunks, channelAllocation);
        break;
    }
    gridFTPClient.waitForTransferCompletion();
    timeSpent += ((System.currentTimeMillis() - start) / 1000.0);
    LOG.info(conf.algorithm.name() +
                    "\tfileClusters\t" + conf.maximumChunks +
                    "\tmaxCC\t" + transferTask.getMaxConcurrency() +
                    " Throughput:" + (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)));
    System.out.println(conf.algorithm.name() +
            " fileClusters: " + conf.maximumChunks +
            " size:" +  Utils.printSize(datasetSize, true) +
            " time:" + timeSpent +
            " thr: "+ (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)));

    isTransferCompleted = true;
    GridFTPClient.executor.shutdown();
    while (!GridFTPClient.executor.isTerminated()) {
    }
    LogManager.close();
    gridFTPClient.stop();
  }

}
