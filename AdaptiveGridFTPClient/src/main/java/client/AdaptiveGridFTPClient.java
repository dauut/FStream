package client;

import client.hysterisis.Entry;
import client.hysterisis.Hysteresis;
import client.utils.*;
import com.google.common.annotations.VisibleForTesting;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
//    private FileCluster newChunk;

    private ArrayList<FileCluster> chunks;
    private static boolean firstPassPast = false;
    public static int TRANSFER_NUMBER = 1;
    private List<SessionParameters> sessionParameters = new ArrayList<>();

    public static HashMap<Integer, Boolean> isTransfersCopmletedMap = new HashMap<>();

    private final static Logger debugLogger = LogManager.getLogger("reportsLogger");

    public static ConcurrentHashMap<ChannelModule.ChannelPair, Boolean> smallMarkedChannels = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<ChannelModule.ChannelPair, Boolean> largeMarkedChannels = new ConcurrentHashMap<>();

    public static HashSet<ChannelModule.ChannelPair> channelInUse = new HashSet<>();

    private Thread monitorThisTransfer;
    private Thread checkDataThread;
    public static long dataSizeofCurrentTransfer = 0;

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
//        adaptiveGridFTPClient.checkNewData();
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(() -> {
            // do stuff
            try {
                adaptiveGridFTPClient.checkNewData();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, 0, 5, TimeUnit.SECONDS);
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
            for (int i = 0; i < dataset.getFileList().size(); i++) {
                if (!allFiles.contains(dataset.getFileList().get(i).fullPath())) {
                    allFiles.add(dataset.getFileList().get(i).fullPath());
                    isNewFile = true;
                }
            }
            System.out.println("isNewFile = " + isNewFile);
//            }
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

    public void checkNewData() throws InterruptedException {
        if (dataNotChangeCounter < 1000) {
            Thread.sleep(10 * 1000); //wait for X sec. before next check
            System.err.println("Checking data counter = " + dataNotChangeCounter);
            lookForNewData();
            if (isNewFile) {
                dataNotChangeCounter = 0;
//                return;
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

        cleanCurrentTransferInformation();

        if (dataNotChangeCounter >= 200) {
            isTransferCompleted = true;
            GridFTPClient.executor.shutdown();
            while (!GridFTPClient.executor.isTerminated()) {
            }
            gridFTPClient.stop();
        }
    }

    public void cleanCurrentTransferInformation() {
        /*Current transfer completed. Clean session parameters. */
        for (FileCluster chunk : chunks) {
            chunk.getRecords().totalTransferredSize = 0;
            chunk.getRecords().initialSize = 0;
            chunk.getRecords().channels = null;
            GridFTPClient.ftpClient.fileClusters.remove(chunk);
        }
        dataSizeofCurrentTransfer = 0;
        chunks = null;
    }

    private void addNewFilesToChunks() throws Exception {

        XferList newFiles = newDataset;
        dataSizeofCurrentTransfer += newFiles.size();
        chunks = Utils.createFileClusters(newFiles, chunks, conf.bandwidth, conf.rtt, maximumChunks);
        int[][] estimatedParamsForChunks = new int[chunks.size()][4];
        boolean staticSettings = false;
        if (staticSettings) {
            for (int i = 0; i < estimatedParamsForChunks.length; i++) {
                if (i < sessionParameters.size()) {
                    TunableParameters tunableParameters = new TunableParameters(sessionParameters.get(i).getConcurrency(),
                            sessionParameters.get(i).getParallelism(), sessionParameters.get(i).getPipelining(),
                            sessionParameters.get(i).getBufferSize());
                    chunks.get(i).setTunableParameters(tunableParameters);
                }else{
                    debugLogger.debug("There no initial settings for " + chunks.get(i).getDensity()
                            + " size chunks. Using static one...");
                    TunableParameters tunableParameters = new TunableParameters(sessionParameters.get(0).getConcurrency(),
                            sessionParameters.get(0).getParallelism(), sessionParameters.get(0).getPipelining(),
                            sessionParameters.get(0).getBufferSize());

                    chunks.get(i).setTunableParameters(tunableParameters);
                }

                debugLogger.debug("[STATIC] Chunk: = " + chunks.get(i).getDensity() + "confs: Concurrency: "
                        + chunks.get(i).getTunableParameters().getConcurrency()
                        + " Parallelism: " + chunks.get(i).getTunableParameters().getParallelism()
                        + " Pipelining: " + chunks.get(i).getTunableParameters().getPipelining());
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
//                    sp.setBufferSize();
                    sessionParameters.add(sp);
                    TunableParameters tb = new TunableParameters(tunableParameters[i].getConcurrency(),
                            tunableParameters[i].getParallelism(), tunableParameters[i].getPipelining(),
                            sessionParameters.get(0).getBufferSize());
                    chunks.get(i).setTunableParameters(tb);
                }
                debugLogger.debug("[DYNAMIC] Chunk: " + chunks.get(i).getDensity() + "; [REQ] Concurrency: " + chunks.get(i).getTunableParameters().getConcurrency() +
                        "; Parallelism: " + chunks.get(i).getTunableParameters().getParallelism() +
                        "; Pipelining: " + chunks.get(i).getTunableParameters().getPipelining() + "; Updated file count = " + chunks.get(i).getRecords().getFileList().size());
                System.out.println("[DYNAMIC] Chunk: " + chunks.get(i).getDensity() + "; [REQ] Concurrency: " + chunks.get(i).getTunableParameters().getConcurrency() +
                        "; Parallelism: " + chunks.get(i).getTunableParameters().getParallelism() +
                        "; Pipelining: " + chunks.get(i).getTunableParameters().getPipelining() +
                        "; Updated file count = " + chunks.get(i).getRecords().getFileList().size());

                LOG.info("[SESSION] Chunk " + chunks.get(i).getDensity()
                        + "[CUR] concurrency = " + sessionParameters.get(i).getConcurrency()
                        + "; parallelism = " + sessionParameters.get(i).getParallelism()
                        + "; pipelining = " + sessionParameters.get(i).getPipelining());
                System.out.println("[SESSION] Chunk " + chunks.get(i).getDensity()
                        + "[CUR] concurrency = " + sessionParameters.get(i).getConcurrency()
                        + "; parallelism = " + sessionParameters.get(i).getParallelism()
                        + "; pipelining = " + sessionParameters.get(i).getPipelining());
            }
        }

        LOG.info(" Running MC with :" + transferTask.getMaxConcurrency() + " channels.");
        for (FileCluster chunk : chunks) {
            LOG.info("Chunk :" + chunk.getDensity().name() + " cc:" + chunk.getTunableParameters().getConcurrency() +
                    " p:" + chunk.getTunableParameters().getParallelism() + " ppq:" + chunk.getTunableParameters().getPipelining());
        }

        //assign max allowed channel sizes
        Utils.allocateChannelsToChunks(chunks, transferTask.getMaxConcurrency(), conf.channelDistPolicy);

        int index = 0;
        for (FileCluster chunk : chunks) {
            System.out.println("New files transferring... Chunk = " + chunk.getDensity());

            if (!GridFTPClient.ftpClient.fileClusters.contains(chunk)) {
                GridFTPClient.ftpClient.fileClusters.add(chunk);
            }

            XferList xl = chunk.getRecords();
            xl.initialSize += xl.size();

            if (xl.channels == null || xl.channels.size() == 0) {
                xl.channels = new ArrayList<>();
            }

            synchronized (chunk.getRecords()) {
                xl.updateDestinationPaths();
            }

            boolean isConcurrencyChange = isConcChange(sessionParameters.get(index), xl.channels, chunk.getTunableParameters().getConcurrency());
            boolean isPipeChange = isPipChanged(sessionParameters.get(index), xl.channels);
            boolean isChangeNeed = isConcurrencyChange || isPipeChange;

            if (!isChangeNeed) {
                System.err.println("NO NEED CHANGE FOR: " + chunk.getDensity().toString() +
                        " CHUNK CHANGE. IT WILL TRANSFER WITH CURRENT SETTINGS.");
                index++;
                LOG.info("Current setting remain for " + chunk.getDensity().toString() + " chunk. Continue without change..");
                continue;
            }
            HashSet<ChannelModule.ChannelPair> channelInUseForThisChunk = new HashSet<>(chunk.getRecords().channels);
            System.out.println("channelInUseForThisChunk size = " + channelInUseForThisChunk.size());
            allocateChannelsOnDemand(chunk.getTunableParameters().getConcurrency(),
                    sessionParameters.get(index).getConcurrency(), chunk, isConcurrencyChange, isPipeChange);
            chunk.isReadyToTransfer = true;
            long strt = System.currentTimeMillis();
            boolean isParallelismChange = parallelismChange(sessionParameters.get(index), chunk);
            if (isParallelismChange) {
            }
            long end=0;
            end+=(System.currentTimeMillis() - strt) / 1000;
            debugLogger.debug("Ch count = " + chunk.getRecords().channels.size() + " parallelism settings time = " + end);
            System.out.println("Transfer chunk type = " + chunk.getDensity().toString() +
                    " session params type = " + sessionParameters.get(index).getChunkType());
            System.out.println("Transfer started = " + chunk.getDensity().toString() + " need for = " +
                    sessionParameters.get(index).getConcurrency()
                    + " allowed max = " + chunk.getTunableParameters().getConcurrency() + " allocated = " +
                    xl.channels.size());

//            LOG.info("************ADDITIONAL TRANSFER RELAUNCH WITH: " + xl.channels.size());
            availableChannelCount(chunk, sessionParameters.get(index));
            for (int i = 0; i < xl.channels.size(); i++) {
                if (!channelInUseForThisChunk.contains(xl.channels.get(i))) {
                    channelInUse.add(xl.channels.get(i));
                    Runnable runs = new RunTransfers(xl.channels.get(i));
                    GridFTPClient.executor.submit(runs);
                }
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

//        if (checkDataThread == null || !checkDataThread.isAlive()) {
//            System.out.println("Check data thread is starting...");
//            checkDataThread = new CheckNewData(this);
//            checkDataThread.start();
//        } else {
//            System.out.println("Check new data thread is alive..");
//        }

//        monitorThisTransfer.join();
    }

    private void allocateChannelsOnDemand(int maxConcForThisChunk, int desiredConcurrency, FileCluster chunk,
                                          boolean isConcurrencyChange, boolean isPipeChange) {
        System.err.println("----------CHANNEL DISTRIBUTION ON DEMAND STARTED... ");
        String chunkType = chunk.getDensity().toString();
        System.out.println("Chunk Type = " + chunkType + " IN NEED:"
                + desiredConcurrency + "; Current channel size:" + chunk.getRecords().channels.size()
                + "MAX ALLOWED : " + maxConcForThisChunk);

        ConcurrentHashMap<ChannelModule.ChannelPair, Boolean> channelsDemanded = retDemand(chunkType);
        ConcurrentHashMap<ChannelModule.ChannelPair, Boolean> otherChannels = retOther(chunkType);

        boolean channelsDistributed = false;

        if (desiredConcurrency > maxConcForThisChunk) {
            desiredConcurrency = maxConcForThisChunk;
        }

        // if we have more channel than required
        // we need to reduce to channel count.
        // mark channels as remove, then wait for the complete current transfer on that particular channels.
        // then remove from list.
        // part of this job is ongoing in transfer() method.
        if (chunk.getRecords().channels.size() != 0 && desiredConcurrency < chunk.getRecords().channels.size()) {
            System.out.println("Current channel size is bigger than we need. Reducing count of " +
                    (chunk.getRecords().channels.size() - desiredConcurrency) + " channels.");

            ArrayList<ChannelModule.ChannelPair> list = new ArrayList<>();
            for (int i = 0; i < chunk.getRecords().channels.size() - desiredConcurrency; i++) {
                System.out.println("Channel = " + chunk.getRecords().channels.get(i) + " going to mark as remove..");
                synchronized (chunk.getRecords().channels.get(i)) {
                    System.out.println("[SYNC] Channel = " + chunk.getRecords().channels.get(i) + " going to mark as remove..");
                    chunk.getRecords().channels.get(i).setMarkedAsRemove(true);
                    list.add(chunk.getRecords().channels.get(i));
                }
            }
            System.out.println(chunk.getRecords().channels.size() - desiredConcurrency + " channel marked as remove.");
            while (list.size() != 0) {
                if (list.get(0).inTransitFiles.size() == 0) {
                    list.remove(0);
                }
            }

            System.out.println("Channels decreased to size = " + chunk.getRecords().channels.size());

            if (isPipeChange) {
                System.out.println("Pipelining change need. Old pipelining: "
                        + chunk.getRecords().channels.get(0).getPipelining()
                        + " new pipelining = " + chunk.getTunableParameters().getPipelining());

                for (int i = 0; i < chunk.getRecords().channels.size(); i++) {
                    if (chunk.getRecords().channels.get(i).getPipelining() != chunk.getTunableParameters().getPipelining()) {
                        chunk.getRecords().channels.get(i).setPipelining(chunk.getTunableParameters().getPipelining());
                    }
                }
            }
            return;
        }

        // only pipe changes so sync and change.
        if (!isConcurrencyChange && isPipeChange) {
            System.out.println("Only pipeline change need. Changing. Old pipelining = "
                    + chunk.getRecords().channels.get(0).getPipelining() +
                    " new pipeline = " + chunk.getTunableParameters().getPipelining());
            changePipeline(chunk); // helper
            return;
        }

        if (chunk.getRecords().channels.size() != 0 && desiredConcurrency > chunk.getRecords().channels.size()) {
            desiredConcurrency = desiredConcurrency - chunk.getRecords().channels.size();
        }

        // if we come so far, we need to add new channels. so pick channels from available sets.
        // if there is not enough available channels then satisfy as much as we can.
        System.out.println("Need abs channel count = " + desiredConcurrency);

        int index = 0;

//        availableChannelCount(channelsDemanded,otherChannels);

        long start = System.currentTimeMillis();
        long timeSpent = 0;
        int desiredCounter = 0;
        while (!channelsDistributed) {
            boolean needOtherChannels = false;
            while (index < maxConcForThisChunk && index < desiredConcurrency && !needOtherChannels) {
                if (channelsDemanded != null) {
                    for (ChannelModule.ChannelPair cp : channelsDemanded.keySet()) {
                        if (!channelsDemanded.get(cp)) {
                            cp.chunk = chunk;
                            chunk.getRecords().channels.add(cp);
                            channelsDemanded.put(cp, true);
                            index++;
                        }
                        if (index == maxConcForThisChunk || index == desiredConcurrency) {
                            break;
                        }
                    }
                    if (index < maxConcForThisChunk && index < desiredConcurrency) {
                        System.err.println("Looking for " + (desiredConcurrency - index) + " more channels");
                        needOtherChannels = true;
                    } else {
                        channelsDistributed = true;
                    }

                }
            }

            desiredCounter++;
            if (desiredCounter == 500) {
                System.err.println("Channel allocation break with distribution =  " + channelsDistributed);
                break;
            }

            if (needOtherChannels) {
                if (otherChannels != null) {
                    for (ChannelModule.ChannelPair cp : otherChannels.keySet()) {
                        if (!otherChannels.get(cp)) {
                            cp.chunk = chunk;
                            cp.setChunkType(chunkType);
                            chunk.getRecords().channels.add(cp);
                            index++;
                            otherChannels.remove(cp);
                            channelsDemanded.put(cp, true);
                            if (index == maxConcForThisChunk || index == desiredConcurrency) {
                                break;
                            }
                        }
                    }
                    channelsDistributed = true;
                }
            }
        }
        if (isPipeChange) {
            System.out.println("Concurrency change done. Pipelining change...");
            changePipeline(chunk);
            System.out.println("Pipelining change done.");
        }
        timeSpent += ((System.currentTimeMillis() - start) / 1000.0);
        System.err.println(chunk.getRecords().channels.size() + " channel for " + chunkType + " chunk allocated." +
                " Time: " + timeSpent);
    }

    private void changePipeline(FileCluster chunk) {
        for (ChannelModule.ChannelPair c : chunk.getRecords().channels) {
            synchronized (c) {
                c.setPipelining(chunk.getTunableParameters().getPipelining());
            }
        }
    }

    private void availableChannelCount(FileCluster chunk, SessionParameters sessionParameters) {
        System.err.println("Requested settings: paral" + sessionParameters.getParallelism() + "; pipe: " + sessionParameters.getPipelining());
        System.err.println("CHANNEL SETTINGS BEFORE TRANSFER: ");
        for (ChannelModule.ChannelPair c : chunk.getRecords().channels) {
            System.err.println("Channel " + c.getId() + "; paral: " + c.parallelism + "; pipe: " + c.getPipelining());
        }
    }

    private boolean isPipChanged(SessionParameters sessionParameters, List<ChannelModule.ChannelPair> channels) {
        int currentConc = channels.size();
        if (currentConc == 0) {
            return true;
        }
        int curPipelining = channels.get(0).getPipelining();
        int requestedPipelining = sessionParameters.getPipelining();
        if (requestedPipelining != curPipelining) {
            System.out.println("Pipelining change need because: old = " + curPipelining + " new = " + requestedPipelining);
            return true;
        }
        return false;
    }

    private boolean isConcChange(SessionParameters sessionParameters, List<ChannelModule.ChannelPair> channels, int maxAllowedConc) {
        int currentConc = channels.size();
        if (currentConc == 0) {
            return true;
        }
        if (maxAllowedConc == currentConc) {
            return false;
        }
        int requestedNewConcurrency = sessionParameters.getConcurrency();
        if (currentConc != requestedNewConcurrency) {
            System.out.println("Concurrency change need because: old = " + currentConc + " new = " + requestedNewConcurrency);
            return true;
        }
        return false;
    }

    private boolean parallelismChange(SessionParameters sessionParameters, FileCluster chunk) {
        int requestedParallelism = sessionParameters.getParallelism();
        int i;
        boolean needChange = false;
        for (i = chunk.getRecords().channels.size() - 1; i >=0 ; i--) {
            System.out.println("i = " + " channel " + chunk.getRecords().channels.get(i).getId() + " paral = " + chunk.getRecords().channels.get(i).parallelism);
            if (chunk.getRecords().channels.get(i).parallelism != requestedParallelism) {
                ChannelModule.ChannelPair cp = chunk.getRecords().channels.get(i);
                System.out.println("Channel " + cp.getId() + " need restart. Cur parallelism = " + cp.parallelism + " need for = " + requestedParallelism);
                cp.newChunk = chunk;
                cp = restartChannel(cp);
                System.out.println("After restart parallelism = " + cp.parallelism);
                needChange = true;
            }
        }
        if (needChange) {
            System.out.println("Restart channellls. ");
        }

        return needChange;
    }

    private synchronized ChannelModule.ChannelPair restartChannel(ChannelModule.ChannelPair oldChannel) {
        System.out.println("Updating channel " + oldChannel.getId() + " parallelism to " +
                oldChannel.newChunk.getTunableParameters().getParallelism());
        XferList oldFileList = oldChannel.chunk.getRecords();
        XferList newFileList = oldChannel.newChunk.getRecords();
        XferList.MlsxEntry fileToStart = getNextFile(newFileList);
        if (fileToStart == null) {
            return null;
        }

        synchronized (oldFileList) {
            oldFileList.channels.remove(oldChannel);
        }

        ChannelModule.ChannelPair newChannel;
//        if (Math.abs(oldChannel.chunk.getTunableParameters().getParallelism() -
//                oldChannel.newChunk.getTunableParameters().getParallelism()) > 0) {
        oldChannel.setMarkedAsRemove(true);
        System.out.println("*** old channel marked as remove.... ");
        while(oldChannel.inTransitFiles.size()!=0){}
        System.out.println("OLD Channel intransit size = " + oldChannel.inTransitFiles.size());
        oldChannel.close();
        newChannel = new ChannelModule.ChannelPair(GridFTPClient.su, GridFTPClient.du);
        boolean success = GridFTPClient.setupChannelConf(newChannel, oldChannel.getId(), oldChannel.newChunk, fileToStart);
        if (!success) {
            synchronized (newFileList) {
                newFileList.addEntry(fileToStart);
                return null;
            }
        }
//        }
        /*else {
            oldChannel.chunk = oldChannel.newChunk;
            oldChannel.setPipelining(oldChannel.newChunk.getTunableParameters().getPipelining());
            oldChannel.pipeTransfer(fileToStart);
            oldChannel.inTransitFiles.add(fileToStart);
            newChannel = oldChannel;
        }*/

        synchronized (newFileList.channels) {
            newFileList.channels.add(newChannel);
        }
        updateOnAir(newFileList, +1);
        System.out.println("restartChannel end  = " + newChannel.parallelism);
        return newChannel;
    }

    private XferList.MlsxEntry getNextFile(XferList fileList) {
        synchronized (fileList) {
            if (fileList.count() > 0) {
                return fileList.pop();
            }
        }
        return null;
    }

    private void updateOnAir(XferList fileList, int count) {
        synchronized (fileList) {
            fileList.onAir += count;
        }
    }

    /*
     * This two helpers are for returning correct channel set for allocation.
     * */
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
