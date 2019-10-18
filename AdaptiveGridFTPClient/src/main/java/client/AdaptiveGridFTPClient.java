package client;

import client.hysterisis.Entry;
import client.hysterisis.Hysteresis;
import client.utils.HostResolution;
import client.utils.RunTransfers;
import client.utils.TunableParameters;
import client.utils.Utils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import transfer_protocol.module.ChannelModule;
import transfer_protocol.module.GridFTPClient;
import transfer_protocol.util.XferList;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static client.ConfigurationParams.maximumChunks;

public class AdaptiveGridFTPClient {

    public static Entry transferTask;
    public static boolean isTransferCompleted = false;
    private GridFTPClient gridFTPClient;
    Hysteresis hysteresis;
    ConfigurationParams conf;
    private static final Log LOG = LogFactory.getLog(AdaptiveGridFTPClient.class);
    //
    private int dataNotChangeCounter = 0;
    private XferList newDataset;
    private HashSet<String> allFiles = new HashSet<>();
    private boolean isNewFile = false;
    private ArrayList<FileCluster> tmpchunks = null;
    private ArrayList<FileCluster> chunks;
    private static boolean firstPassPast = false;
    private static int TRANSFER_NUMBER = 1;
    private List<TunableParameters> staticTunableParams = new ArrayList<>();
    private boolean staticSettings = true;

    private final static Logger debugLogger = LogManager.getLogger("reportsLogger");

    public AdaptiveGridFTPClient() {
        //initialize output streams for message logging
        conf = new ConfigurationParams();
    }

    @VisibleForTesting
    public AdaptiveGridFTPClient(GridFTPClient gridFTPClient) {
        this.gridFTPClient = gridFTPClient;
    }

    public static void main(String[] args) throws Exception {
        AdaptiveGridFTPClient adaptiveGridFTPClient = new AdaptiveGridFTPClient();
        adaptiveGridFTPClient.parseArguments(args); //parse arguments
        adaptiveGridFTPClient.initConnection(); //init connection
        adaptiveGridFTPClient.lookForNewData(); // first time look
        firstPassPast = true;

        Thread streamThread = new Thread(() -> {
            try {
                adaptiveGridFTPClient.transfer();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        streamThread.start();

        Thread checkDataPeriodically = new Thread(() -> {
            try {
                adaptiveGridFTPClient.checkNewData();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        checkDataPeriodically.start();
        checkDataPeriodically.join();

    }

    private void initConnection() {
//        debugLogger.debug("NEW EXPERIMENT STARTED............... with staticsetttings: " + staticSettings);
        transferTask = new Entry();
        transferTask.setSource(conf.source);
        transferTask.setDestination(conf.destination);
        transferTask.setBandwidth(conf.bandwidth);
        transferTask.setRtt(conf.rtt);
        transferTask.setBDP((transferTask.getBandwidth() * transferTask.getRtt()) / 8); // In MB
        transferTask.setBufferSize(conf.bufferSize);
        transferTask.setMaxConcurrency(conf.maxConcurrency);
        LOG.info("*************" + conf.algorithm + "************");

        URI su = null, du = null; //url paths
        try {
            su = new URI(transferTask.getSource()).normalize();
            du = new URI(transferTask.getDestination()).normalize();
        } catch (URISyntaxException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        HostResolution sourceHostResolution = new HostResolution(su.getHost());
        HostResolution destinationHostResolution = new HostResolution(du.getHost());
        sourceHostResolution.start();
        destinationHostResolution.start();

        // create Control Channel to source and destination server
        if (gridFTPClient == null) {
            gridFTPClient = new GridFTPClient(conf.source, conf.destination, conf.proxyFile);
            gridFTPClient.start();
            gridFTPClient.waitFor();
        }

        //
        if (gridFTPClient == null || GridFTPClient.ftpClient == null) {
            LOG.info("Could not establish GridFTP connection. Exiting...");
            System.exit(-1);
        }

        //Additional transfer configurations
        gridFTPClient.useDynamicScheduling = conf.useDynamicScheduling;
        gridFTPClient.useOnlineTuning = ConfigurationParams.useOnlineTuning;
        gridFTPClient.setPerfFreq(conf.perfFreq);
        GridFTPClient.ftpClient.setEnableIntegrityVerification(conf.enableIntegrityVerification);

    }

    /*
     * This method basically checking the source destination for new data.
     * Global variable used due to passing data information to transfer channel
     * */
    private void lookForNewData() {

        //Get metadata information of dataset
        XferList dataset = null;

        try {
            // check data
            dataset = gridFTPClient.getListofFiles(allFiles);

            //if there is data then cont.
            if (dataset.getFileList().size() == 0) {
                isNewFile = false;
            } else {
                for (int i = 0; i < dataset.getFileList().size(); i++) {
                    if (!allFiles.contains(dataset.getFileList().get(i).fileName)) {
                        allFiles.add(dataset.getFileList().get(i).fileName);
                        isNewFile = true;
                    } else {
                        isNewFile = false;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        newDataset = dataset; // assign most recent dataset
        if (isNewFile && firstPassPast) {
            try {
                addNewFilesToChunks();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private void checkNewData() throws InterruptedException {
        while (dataNotChangeCounter < 1000) {
            Thread.sleep(10 * 2000); //wait for X sec. before next check
            System.err.println("Checking data counter = " + dataNotChangeCounter);
            lookForNewData();
            if (isNewFile) {
                dataNotChangeCounter = 0;
                return;
            } else {
                dataNotChangeCounter++;
            }
        }
    }

    private void parseArguments(String[] arguments) {
        conf.parseArguments(arguments, transferTask);

    }

    @VisibleForTesting
    private void transfer() throws Exception {

        double startTime = System.currentTimeMillis();

        //First fetch the list of files to be transferred
        XferList dataset = newDataset;
        long datasetSize = dataset.size();

        LOG.info("file listing completed at:" + ((System.currentTimeMillis() - startTime) / 1000.0) +
                " data size:" + Utils.printSize(datasetSize, true));
        chunks = Utils.createFileClusters(dataset, tmpchunks, conf.bandwidth, conf.rtt, maximumChunks);
        tmpchunks = chunks;

        if (conf.useHysterisis) {
            // Initialize historical data analysis
            hysteresis = new Hysteresis();
            hysteresis.findOptimalParameters(chunks, transferTask);
        }

        int[][] estimatedParamsForChunks = new int[chunks.size()][4];
        long timeSpent = 0;
        long start;
        gridFTPClient.startTransferMonitor();

        // Make sure total channels count does not exceed total file count
//        int totalChannelCount = Math.min(conf.maxConcurrency, dataset.count());

        if (conf.useHysterisis) {
            int maxConcurrency = 0;
            for (int i = 0; i < estimatedParamsForChunks.length; i++) {
                //fileClusters.get(i).getRecords().setTransferParameters(estimatedParamsForChunks[i]);
                if (estimatedParamsForChunks[i][0] > maxConcurrency) {
                    maxConcurrency = estimatedParamsForChunks[i][0];
                }
            }
        } else {
            for (int i = 0; i < estimatedParamsForChunks.length; i++) {
                staticTunableParams.add(Utils.getBestParams(chunks.get(i).getRecords(), maximumChunks));
                chunks.get(i).setTunableParameters(Utils.getBestParams(chunks.get(i).getRecords(), maximumChunks));
                debugLogger.debug("Initial confs = " + "Concurrency: " + chunks.get(i).getTunableParameters().getConcurrency() +
                        " Parallelism: " + chunks.get(i).getTunableParameters().getParallelism() +
                        " Pipelining: " + chunks.get(i).getTunableParameters().getPipelining());
            }
        }

        LOG.info(" Running MC with :" + transferTask.getMaxConcurrency() + " channels.");

        for (FileCluster chunk : chunks) {
            LOG.info("Chunk :" + chunk.getDensity().name() + " cc:" + chunk.getTunableParameters().getConcurrency() +
                    " p:" + chunk.getTunableParameters().getParallelism() + " ppq:" + chunk.getTunableParameters().getPipelining());
        }

        Utils.allocateChannelsToChunks(chunks, transferTask.getMaxConcurrency(), conf.channelDistPolicy);

        for (FileCluster fileCluster : chunks) {
            gridFTPClient.runTransfer(fileCluster);
        }

        start = System.currentTimeMillis();

        gridFTPClient.waitForTransferCompletion();

        timeSpent += ((System.currentTimeMillis() - start) / 1000.0);
        System.err.println("FIRST TRANSFER COMPLETED! in " + timeSpent + " seconds.");
        debugLogger.debug(timeSpent + "\t" + (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)));
        LOG.info(conf.algorithm.name() +
                "\tfileClusters\t" + maximumChunks +
                "\tmaxCC\t" + transferTask.getMaxConcurrency() +
                " Throughput:" + (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)));
        System.out.println(conf.algorithm.name() +
                " fileClusters: " + maximumChunks +
                " size:" + Utils.printSize(datasetSize, true) +
                " time:" + timeSpent +
                " thr: " + (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)));

        for (FileCluster chunk: chunks){
            chunk.getRecords().totalTransferredSize = 0;
            System.out.println("Transfers completed remove chunks...");
//            GridFTPClient.ftpClient.fileClusters.remove(chunk);
        }

        if (dataNotChangeCounter >= 200) {
            isTransferCompleted = true;
            GridFTPClient.executor.shutdown();
            while (!GridFTPClient.executor.isTerminated()) {
            }
            gridFTPClient.stop();
        }
    }

    private void addNewFilesToChunks() throws Exception {
        XferList newFiles = newDataset;
        long datasetSize = newFiles.size();
        synchronized (chunks) {
            chunks = Utils.createFileClusters(newFiles, chunks, conf.bandwidth, conf.rtt, maximumChunks);
        }

        int[][] estimatedParamsForChunks = new int[chunks.size()][4];
        if (staticSettings) {
            for (int j = 0; j < estimatedParamsForChunks.length; j++) {
                chunks.get(j).setTunableParameters(staticTunableParams.get(j));
                debugLogger.debug("STATIC confs = " + "Concurrency: " + chunks.get(j).getTunableParameters().getConcurrency() +
                        " Parallelism: " + chunks.get(j).getTunableParameters().getParallelism() +
                        " Pipelining: " + chunks.get(j).getTunableParameters().getPipelining());
            }
        } else {
            TunableParameters tunableParameters = Utils.getBestParams(chunks.get(0).getRecords(), maximumChunks);

            for (ChannelModule.ChannelPair cc:GridFTPClient.TransferChannel.channelPairList){
                cc.setPipelining(tunableParameters.getPipelining()); //currently we have only one chunk. It will change in the future.
            }

            for (int j = 0; j < estimatedParamsForChunks.length; j++) {

                staticTunableParams.get(0).setPipelining(tunableParameters.getPipelining());
                staticTunableParams.get(0).setConcurrency(tunableParameters.getConcurrency());
                chunks.get(j).setTunableParameters(staticTunableParams.get(0));

                //dynamic params set
                //chunks.get(j).setTunableParameters(Utils.getBestParams(chunks.get(j).getRecords(), maximumChunks));

                debugLogger.debug("Conf. changed: new confs = " + "Concurrency: " + chunks.get(j).getTunableParameters().getConcurrency() +
                        " Parallelism: " + chunks.get(j).getTunableParameters().getParallelism() +
                        " Pipelining: " + chunks.get(j).getTunableParameters().getPipelining());
            }

        }
        LOG.info(" Running MC with :" + transferTask.getMaxConcurrency() + " channels.");
        for (FileCluster chunk : chunks) {
            LOG.info("Chunk :" + chunk.getDensity().name() + " cc:" + chunk.getTunableParameters().getConcurrency() +
                    " p:" + chunk.getTunableParameters().getParallelism() + " ppq:" + chunk.getTunableParameters().getPipelining());
        }
        Utils.allocateChannelsToChunks(chunks, transferTask.getMaxConcurrency(),conf.channelDistPolicy);

        long start = System.currentTimeMillis();
        long timeSpent =0 ;
        for (FileCluster chunk : chunks) {
            System.out.println("New files transferring... Chunk = " + chunk.getDensity());
            XferList xl = chunk.getRecords();
            xl.initialSize = xl.size();
            synchronized (chunk) {
                xl.updateDestinationPaths();
            }

            if(!GridFTPClient.ftpClient.fileClusters.contains(chunk)){
                System.out.println("Chunk is not in FTPCLIENT adding");
                GridFTPClient.ftpClient.fileClusters.add(chunk);
            }

            for (ChannelModule.ChannelPair cc:GridFTPClient.TransferChannel.channelPairList){
                cc.chunk = chunk;
            }

            xl.channels = GridFTPClient.TransferChannel.channelPairList;
            chunk.isReadyToTransfer = true;
            for (int i = 0;i<chunk.getTunableParameters().getConcurrency() ; i++) {
                Runnable runs = new RunTransfers(xl.channels.get(i));
                GridFTPClient.executor.submit(runs);
            }
        }

        GridFTPClient.waitEndOfTransfer();
        timeSpent += ((System.currentTimeMillis() - start) / 1000.0);
        System.err.println("TRANSFER NUM = " + TRANSFER_NUMBER + " is COMPLETED! in " + timeSpent + " seconds.");
        debugLogger.debug(timeSpent + "\t" + (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)));
        for (FileCluster chunk: chunks){
            chunk.getRecords().totalTransferredSize = 0;
            System.out.println("Transfers completed remove chunks2...");
//            GridFTPClient.ftpClient.fileClusters.remove(chunk);
        }
        TRANSFER_NUMBER++;
        checkNewData();
    }

    private void channelConfForNewFiles(ChannelModule.ChannelPair cc,
                                        int channelId,
                                        FileCluster chunk,
                                        XferList.MlsxEntry firstFileToTransfer) {
        //it optimized for 1 chunk for now.
        // TODO: improve that for more than 1 chunk
//        debugLogger.debug("Channel configurations for new files------- ");
        TunableParameters params;
        if (staticSettings) {
            params = staticTunableParams.get(0);
        } else {
            chunk.setTunableParameters(Utils.getBestParams(chunks.get(0).getRecords(), maximumChunks));
            params = chunk.getTunableParameters();
        }

        cc.chunk = chunk;

        try {
            if (!staticSettings) {
                cc.setParallelism(params.getParallelism());
                cc.pipelining = params.getPipelining();
                LOG.info(" CONF CHANGED: pipelining = " + cc.getPipelining() + " parallism = " + cc.parallelism + "concurrency = " + transferTask.getMaxConcurrency());

            }
            cc.setID(channelId);
            cc.pipeTransfer(firstFileToTransfer);
            cc.inTransitFiles.add(firstFileToTransfer);
//            if (cc.inTransitFiles.peek()!=null) {
//                debugLogger.debug("File Path: " + cc.inTransitFiles.peek().spath);
//            }
        } catch (Exception e) {
            System.out.println("Failed to setup new files channels. ");
            e.printStackTrace();
        }
    }

    private int[] allocateChannelsToChunks(List<FileCluster> chunks, final int channelCount) {
        int totalChunks = chunks.size();
        int[] fileCount = new int[totalChunks];
        for (int i = 0; i < totalChunks; i++) {
            fileCount[i] = chunks.get(i).getRecords().count();
        }

        int[] concurrencyLevels = new int[totalChunks];
        int modulo = (totalChunks + 1) / 2;
        int count = 0;
        for (int i = 0; count < channelCount; i++) {
            int index = i % modulo;
            if (concurrencyLevels[index] < fileCount[index]) {
                concurrencyLevels[index]++;
                count++;
            }
            if (index < totalChunks - index - 1 && count < channelCount
                    && concurrencyLevels[totalChunks - index - 1] < fileCount[totalChunks - index - 1]) {
                concurrencyLevels[totalChunks - index - 1]++;
                count++;
            }
        }

        for (int i = 0; i < totalChunks; i++) {
            System.out.println("Chunk " + i + ":" + concurrencyLevels[i] + "channels");
        }

        return concurrencyLevels;
    }

    //helper
    public XferList.MlsxEntry synchronizedPop(List<XferList.MlsxEntry> fileList) {
        synchronized (fileList) {
            return fileList.remove(0);
        }
    }

}
