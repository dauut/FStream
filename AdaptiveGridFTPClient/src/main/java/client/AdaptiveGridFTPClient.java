package client;

import client.hysterisis.Entry;
import client.hysterisis.Hysteresis;
import client.utils.MonitorTransfer;
import client.utils.RunTransfers;
import client.utils.TunableParameters;
import client.utils.Utils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.axis.Part;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import transfer_protocol.module.ChannelModule;
import transfer_protocol.module.GridFTPClient;
import transfer_protocol.util.SessionParameters;
import transfer_protocol.util.XferList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static client.ConfigurationParams.maximumChunks;

public class AdaptiveGridFTPClient {

    public static Entry transferTask;
    public static boolean isTransferCompleted = false;
    private GridFTPClient gridFTPClient;
    private Hysteresis hysteresis;
    private ConfigurationParams conf;
    private static final Log LOG = LogFactory.getLog(AdaptiveGridFTPClient.class);
    //
    private int dataNotChangeCounter = 0;
    private XferList newDataset;
    private HashSet<String> allFiles = new HashSet<>();
    private static boolean isNewFile = false;
    private ArrayList<FileCluster> tmpchunks = null;

    private ArrayList<FileCluster> chunks;
    private static boolean firstPassPast = false;
    public static int TRANSFER_NUMBER = 1;
    private List<SessionParameters> sessionParameters = new ArrayList<>();
//    private List<SessionParameters> prevSessionParams = new ArrayList<>();

    public static HashMap<Integer, Boolean> isTransfersCopmletedMap = new HashMap<>();

    private final static Logger debugLogger = LogManager.getLogger("reportsLogger");

    public static int inUseChannelCount = 0;

    public static ConcurrentHashMap<ChannelModule.ChannelPair, Boolean> smallMarkedChannels = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<ChannelModule.ChannelPair, Boolean> largeMarkedChannels = new ConcurrentHashMap<>();

    private Thread monitorThisTransfer;

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
        streamThread.join();
        adaptiveGridFTPClient.checkNewData();
    }

    private void initConnection() {
        transferTask = new Entry();
        transferTask.setSource(conf.source);
        transferTask.setDestination(conf.destination);
        transferTask.setBandwidth(conf.bandwidth);
        transferTask.setRtt(conf.rtt);
        transferTask.setBDP((transferTask.getBandwidth() * transferTask.getRtt()) / 8); // In MB
        transferTask.setBufferSize(conf.bufferSize);
        transferTask.setMaxConcurrency(conf.maxConcurrency);
        LOG.info("*************" + conf.algorithm + "************");

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
            if (gridFTPClient == null) {
                System.out.println("Client is null.");
            }

            dataset = gridFTPClient.getListofFiles(allFiles);
            //if there is data then cont.
            isNewFile = false;
//            if (dataset.getFileList().size() == 0) {
//                isNewFile = false;
//            } else {
            for (int i = 0; i < dataset.getFileList().size(); i++) {
                if (!allFiles.contains(dataset.getFileList().get(i).fileName)) {
                    allFiles.add(dataset.getFileList().get(i).fileName);
                    isNewFile = true;
                }
            }
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (dataset != null) {
            System.err.println("Dataset size " + dataset.getFileList().size() + " isNewFile = " + isNewFile + " firstpass = " + firstPassPast);
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
            Thread.sleep(10 * 1000); //wait for X sec. before next check
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
        if (dataset.getFileList().size() == 0) {
            System.err.print("No files found. System exit.");
            System.exit(0);
        }
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
                if (estimatedParamsForChunks[i][0] > maxConcurrency) {
                    maxConcurrency = estimatedParamsForChunks[i][0];
                }
            }
        } else {
            for (int i = 0; i < estimatedParamsForChunks.length; i++) {
                SessionParameters sp = new SessionParameters();
                TunableParameters tb = Utils.getBestParams(chunks.get(i).getRecords(), maximumChunks);
                chunks.get(i).setTunableParameters(tb);
                sp.setConcurrency(chunks.get(i).getTunableParameters().getConcurrency());
                sp.setParallelism(chunks.get(i).getTunableParameters().getParallelism());
                sp.setPipelining(chunks.get(i).getTunableParameters().getPipelining());
                sp.setBufferSize(chunks.get(i).getTunableParameters().getBufferSize());
                sp.setChunkType(chunks.get(i).getDensity().toString());
                sessionParameters.add(sp);
//                prevSessionParams.add(sp);
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

        for (FileCluster chunk : chunks) {
            chunk.getRecords().totalTransferredSize = 0;
            chunk.getRecords().initialSize = 0;
            System.out.println("Transfers completed remove chunks...");
            GridFTPClient.ftpClient.fileClusters.remove(chunk);
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

        boolean staticSettings = false;
        if (staticSettings) {
            for (int i = 0; i < estimatedParamsForChunks.length; i++) {
                TunableParameters tunableParameters = new TunableParameters(sessionParameters.get(i).getConcurrency(),
                        sessionParameters.get(i).getParallelism(), sessionParameters.get(i).getPipelining(),
                        sessionParameters.get(i).getBufferSize());

                chunks.get(i).setTunableParameters(tunableParameters);
                debugLogger.debug("[STATIC] Chunk: = " + chunks.get(i).getDensity() + "confs: Concurrency: " + chunks.get(i).getTunableParameters().getConcurrency() +
                        " Parallelism: " + chunks.get(i).getTunableParameters().getParallelism() +
                        " Pipelining: " + chunks.get(i).getTunableParameters().getPipelining());
            }
        } else {
            TunableParameters[] tunableParameters = new TunableParameters[chunks.size()];
            int index = 0;
            for (FileCluster chunk : chunks) {
                tunableParameters[index] = Utils.getBestParams(chunk.getRecords(), maximumChunks);
                index++;
            }
            for (int i = 0; i < estimatedParamsForChunks.length; i++) {
                if (i < sessionParameters.size()) {
                    sessionParameters.get(i).setConcurrency(tunableParameters[i].getConcurrency());
                    sessionParameters.get(i).setPipelining(tunableParameters[i].getPipelining());

                    TunableParameters tb = new TunableParameters(sessionParameters.get(i).getConcurrency(),
                            sessionParameters.get(i).getParallelism(), sessionParameters.get(i).getPipelining(),
                            sessionParameters.get(i).getBufferSize());

                    chunks.get(i).setTunableParameters(tb);
                } else {
                    debugLogger.debug("There no initial settings for " + chunks.get(i).getDensity() + " size chunks. Create new configuration set...");
                    SessionParameters sp = new SessionParameters();
                    sp.setPipelining(tunableParameters[i].getPipelining());
                    sp.setConcurrency(tunableParameters[i].getConcurrency());
                    sp.setParallelism(tunableParameters[i].getParallelism());
                    sp.setChunkType(chunks.get(i).getDensity().toString());
                    sessionParameters.add(sp);
                    TunableParameters tb = new TunableParameters(tunableParameters[i].getConcurrency(),
                            tunableParameters[i].getParallelism(), tunableParameters[i].getPipelining(),
                            sessionParameters.get(0).getBufferSize());
                    chunks.get(i).setTunableParameters(tb);
                }
                debugLogger.debug("[DYNAMIC] Chunk: " + chunks.get(i).getDensity() + "; requested confs: Concurrency: " + chunks.get(i).getTunableParameters().getConcurrency() +
                        " Parallelism: " + chunks.get(i).getTunableParameters().getParallelism() +
                        "; Pipelining: " + chunks.get(i).getTunableParameters().getPipelining() + "; Updated file count = " + chunks.get(i).getRecords().size());
                System.out.println("[DYNAMIC] Chunk: " + chunks.get(i).getDensity() + "; requested confs: Concurrency: " + chunks.get(i).getTunableParameters().getConcurrency() +
                        " Parallelism: " + chunks.get(i).getTunableParameters().getParallelism() +
                        "; Pipelining: " + chunks.get(i).getTunableParameters().getPipelining() + "; Updated file count = " + chunks.get(i).getRecords().size());
            }
        }

        LOG.info(" Running MC with :" + transferTask.getMaxConcurrency() + " channels.");
        for (FileCluster chunk : chunks) {
            LOG.info("Chunk :" + chunk.getDensity().name() + " cc:" + chunk.getTunableParameters().getConcurrency() +
                    " p:" + chunk.getTunableParameters().getParallelism() + " ppq:" + chunk.getTunableParameters().getPipelining());
        }

        Utils.allocateChannelsToChunks(chunks, transferTask.getMaxConcurrency(), conf.channelDistPolicy);

//        long start = System.currentTimeMillis();
//        long timeSpent = 0;
        int index = 0;
        for (FileCluster chunk : chunks) {
            System.out.println("New files transferring... Chunk = " + chunk.getDensity());
            if (!GridFTPClient.ftpClient.fileClusters.contains(chunk)) {
                GridFTPClient.ftpClient.fileClusters.add(chunk);
            }
            XferList xl = chunk.getRecords();
            xl.initialSize += xl.size();
            System.out.println("New initial size  = " + xl.initialSize);
            if(xl.channels == null || xl.channels.size()==0) {
                xl.channels = new ArrayList<>();
            }
            synchronized (chunk.getRecords()) {
                xl.updateDestinationPaths();
            }
            allocateChannelsOnDemand(chunk.getTunableParameters().getConcurrency(), sessionParameters.get(index).getConcurrency(), chunk, xl.channels);

            chunk.isReadyToTransfer = true;
            System.out.println("Transfer chunk type = " + chunk.getDensity().toString() + " session params type = " + sessionParameters.get(index).getChunkType());
            System.out.println("Transfer started = " + chunk.getDensity().toString() + " need for = " + sessionParameters.get(index).getConcurrency()
                    + " allowed max = " + chunk.getTunableParameters().getConcurrency() + " allocated = " + xl.channels.size());

            for (int i = 0; i < xl.channels.size(); i++) {
                Runnable runs = new RunTransfers(xl.channels.get(i));
                GridFTPClient.executor.submit(runs);
            }

            index++;
        }
        if (monitorThisTransfer == null || !monitorThisTransfer.isAlive()) {
            System.out.println("Start monitoring this transfer...");
            monitorThisTransfer = new MonitorTransfer(this);
            monitorThisTransfer.start();
        } else {
            System.out.println("Tracking this transfer is still alive...");
        }
//        checkNewData();
        Thread check = new Thread(() -> {
            try {
                checkNewData();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        check.start();
        check.join();
        monitorThisTransfer.join();
//        timeSpent += ((System.currentTimeMillis() - start) / 1000.0);
//        System.err.println("TRANSFER NUM = " + TRANSFER_NUMBER + " is COMPLETED! in " + timeSpent + " seconds.");
//        debugLogger.debug(timeSpent + "\t" + (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)));
//        for (FileCluster chunk : chunks) {
//            chunk.getRecords().totalTransferredSize = 0;
//            chunk.getRecords().initialSize = 0;
//            GridFTPClient.ftpClient.fileClusters.remove(chunk);
//        }
//        TRANSFER_NUMBER++;
    }

    public void cleanCurrentTransferInformation() {
        for (FileCluster chunk : chunks) {
            chunk.getRecords().totalTransferredSize = 0;
            chunk.getRecords().initialSize = 0;
            GridFTPClient.ftpClient.fileClusters.remove(chunk);
        }
    }

    /* no need */ /*
    private boolean isNewAllocationNeed(int i) {
        int oldConc = prevSessionParams.get(i).getConcurrency();
        int oldParal = prevSessionParams.get(i).getParallelism();
        int oldPipe = prevSessionParams.get(i).getPipelining();

        System.out.println("old conc: " + oldConc + "; old paral: " + oldParal + "; old pipe: " + oldPipe);
        System.out.println("new conc: " + sessionParameters.get(i).getConcurrency() + "; new paral: " + sessionParameters.get(i).getParallelism() + "; new pipe: " + sessionParameters.get(i).getPipelining());
        return sessionParameters.get(i).getConcurrency() != oldConc || sessionParameters.get(i).getParallelism() != oldParal
                || sessionParameters.get(i).getPipelining() != oldPipe;
    }
    */
    private void allocateChannelsOnDemand(int maxConcForThisChunk, int desiredConcurrency, FileCluster chunk,
                                          List<ChannelModule.ChannelPair> currentChannels) {
        System.out.println("----------CHANNEL DISTRIBUTION ON DEMAND STARTED... ");
        String chunkType = chunk.getDensity().toString();
        System.out.println("Chunk Type = " + chunkType + " Channel IN USE : " + inUseChannelCount + " IN NEED:"
                + desiredConcurrency + "; Current channel size:" + currentChannels.size());
        if (currentChannels.size() != 0 && desiredConcurrency > currentChannels.size()) {
            desiredConcurrency = Math.abs(desiredConcurrency - currentChannels.size());
        }
        boolean channelsDistributed = false;
        int index = 0;

        ArrayList<ChannelModule.ChannelPair> channelsForThisChunk = new ArrayList<>();
        ConcurrentHashMap<ChannelModule.ChannelPair, Boolean> channelsDemanded = retDemand(chunkType);

        long start = System.currentTimeMillis();
        long timeSpent = 0;

        while (!channelsDistributed) {
            boolean needOtherChannels = false;
            while (index < maxConcForThisChunk && index < desiredConcurrency && !needOtherChannels) {
                if (channelsDemanded != null) {
                    for (ChannelModule.ChannelPair cp : channelsDemanded.keySet()) {
                        if (!channelsDemanded.get(cp)) {
                            cp.chunk = chunk;
                            cp.setPipelining(chunk.getTunableParameters().getPipelining());
                            channelsForThisChunk.add(cp);
                            index++;
                            channelsDemanded.put(cp, true);
                        }
                        if (index == maxConcForThisChunk || index == desiredConcurrency) {
                            break;
                        }
                    }
                    System.out.println("Channel allocated after firs round (demanded channel set) " + channelsForThisChunk.size());
                    if (index < maxConcForThisChunk && index < desiredConcurrency) {
//                        Thread.sleep(600);
                        System.err.println("Looking for " + (desiredConcurrency - index) + " more channels");
                        needOtherChannels = true;
                    } else {
                        channelsDistributed = true;
                    }

                }
            }
            System.out.println("Demand satisfied : " + channelsDistributed);
            if (needOtherChannels) {
                ConcurrentHashMap<ChannelModule.ChannelPair, Boolean> otherChannels = retOther(chunkType);
                if (otherChannels != null) {
                    for (ChannelModule.ChannelPair cp : otherChannels.keySet()) {
                        if (!otherChannels.get(cp)) {
                            cp.chunk = chunk;
                            cp.setChunkType(chunkType);
                            cp.setPipelining(chunk.getTunableParameters().getPipelining());
                            channelsForThisChunk.add(cp);
                            index++;
                            otherChannels.remove(cp);
                            channelsDemanded.put(cp, true);
                            if (index == maxConcForThisChunk || index == desiredConcurrency) {
                                break;
                            }
                        }
                    }
                    System.out.println("Channel allocated after second round (other channel set): " + channelsForThisChunk.size());
                    channelsDistributed = true;
                }
            }
        }

        chunk.getRecords().channels = channelsForThisChunk;
        timeSpent += ((System.currentTimeMillis() - start) / 1000.0);
        System.err.println("ALLOCATION COMPLETED: FOR = " + chunkType + " CHANNEL COUNTS " + chunk.getRecords().channels.size() +
                " Time: " + timeSpent);

    }

    private ConcurrentHashMap<ChannelModule.ChannelPair, Boolean> retDemand(String chunkType) {
        if (chunkType.equals("SMALL")) {
            return smallMarkedChannels;
        } else {
            return largeMarkedChannels;
        }
    }

    private ConcurrentHashMap<ChannelModule.ChannelPair, Boolean> retOther(String chunkType) {
        if (!chunkType.equals("SMALL")) {
            return smallMarkedChannels;
        } else {
            return largeMarkedChannels;
        }
    }

}
