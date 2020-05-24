package client;

import client.hysterisis.Entry;
import client.transfer.ProfilingOps;
import client.utils.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import transfer_protocol.module.ChannelModule;
import transfer_protocol.module.GridFTPClient;
import transfer_protocol.util.SessionParameters;
import transfer_protocol.util.XferList;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static client.ConfigurationParams.maximumChunks;

public class AdaptiveGridFTPClient {

    public static Entry transferTask;
    public static boolean isTransferCompleted = false;
    private GridFTPClient gridFTPClient;
    private ConfigurationParams conf;
    private static final Log LOG = LogFactory.getLog(AdaptiveGridFTPClient.class);
    //
    private int dataNotChangeCounter = 0;
    private XferList newDataset;
    private HashSet<String> allFiles = new HashSet<>();
    private static boolean isNewFile = false;
    private ArrayList<FileCluster> tmpchunks = null;

    public static ArrayList<FileCluster> chunks;
    private static boolean firstPassPast = false;
    public static int TRANSFER_NUMBER = 1;
    public static HashMap<String, SessionParameters> sessionParametersMap = new HashMap<>();

    public static HashMap<Integer, Boolean> isTransfersCopmletedMap = new HashMap<>();
    public final static Logger debugLogger = LogManager.getLogger("reportsLogger");

    public static ConcurrentHashMap<ChannelModule.ChannelPair, Boolean> smallMarkedChannels = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<ChannelModule.ChannelPair, Boolean> largeMarkedChannels = new ConcurrentHashMap<>();

    public static HashSet<ChannelModule.ChannelPair> channelInUse = new HashSet<>();
    public static HashMap<Integer, ArrayList<ChannelModule.ChannelPair>> channelsWithParallelismCountMap = new HashMap<>();

    private Thread monitorThisTransfer;
    public static long dataSizeofCurrentTransfer = 0;

    private boolean isExtraChannelNeeded = false;
    private int extraChCount = 0;

    public static final boolean printSysOut = true;
    private HashMap<String, Long> newDatasetSizes = new HashMap<>();
    public static boolean firstTransferCompleted = false;
    public static boolean isSwapped = false; //sometimes one of the chunk ends and the other gets its place in arraylist
    private static int profilingCounter = 0;
    public static ArrayList<Double> avgThroughput = new ArrayList<>();
    public static int counterOfProfilingChanger = 0;

    public static HashMap<String, Integer> historicalProfiling = new HashMap<>();

    public AdaptiveGridFTPClient() {
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

    }

    /*
     * This method basically checking the source destination for new data.
     * Global variable used due to passing data information to transfer channel
     * */
    private void lookForNewData() throws IOException {
        //Get metadata information of dataset
        XferList distinguishedNewDataset = new XferList(transferTask.getSource(), transferTask.getDestination());
        XferList dataset = null;
        try {
            if (gridFTPClient == null) {
                System.out.println("Client is null.");
            }

            dataset = gridFTPClient.getListofFiles(allFiles);

            //if there is data then cont.
            //if lists are too large and then it may consume more time to read and compare file lists.
            isNewFile = false;
            for (int i = 0; i < dataset.getFileList().size(); i++) {
                if (allFiles.contains(dataset.getFileList().get(i).fullPath())) {
                    dataset.removeItem(i);
                    i -= 1;
                } else {
                    allFiles.add(dataset.getFileList().get(i).fullPath());
                    distinguishedNewDataset.getFileList().add(dataset.getFileList().get(i));
                    isNewFile = true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        newDataset = dataset; // assign most recent dataset

        if (newDataset != null && isNewFile) {
            TunableParameters overAllParams = Utils.getBestParams(newDataset, 1);
            debugLogger.debug("OverallParamsForThisFileSet = " + overAllParams.toString());
        }

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
            Thread.sleep(5 * 1000); //wait for X sec. before next check
            System.err.println("Checking data counter = " + dataNotChangeCounter);
            if (dataNotChangeCounter < 10)
                LOG.info("Checking data counter = " + dataNotChangeCounter);
            try {
                lookForNewData();
            } catch (IOException e) {
                e.printStackTrace();
            }

            if ((ConfigurationParams.profiling || ConfigurationParams.limitedTransfer) && (profilingCounter > 1 &&
                    chunks != null & profilingCounter % 2 == 0 && !ConfigurationParams.isStaticTransfer)) {

                int[][] estimatedParamsForChunks = new int[chunks.size()][4];
                tuneNewChunkParameters(estimatedParamsForChunks);

                if (ConfigurationParams.limitedTransfer) {
//                    qosProfiling();
                    ProfilingOps.qosProfiling(conf.maxConcurrency, conf.channelDistPolicy);
                } else {
//                    chunkProfiling();
                    ProfilingOps.chunkProfiling(conf.maxConcurrency, conf.channelDistPolicy);
                }

                for (FileCluster chunk : chunks) {
                    HashSet<ChannelModule.ChannelPair> channelInUseForThisChunk = new HashSet<>(chunk.getRecords().channels);
                    boolean isConcurrencyChange = isConcChange(sessionParametersMap.get(chunk.getDensity().toString()),
                            chunk.getRecords().channels, chunk.getTunableParameters().getConcurrency());
                    boolean isPipeChange = isPipChanged(sessionParametersMap.get(chunk.getDensity().toString()),
                            chunk.getRecords().channels);
                    allocateChannelsOnDemand(chunk.getTunableParameters().getConcurrency(),
                            sessionParametersMap.get(chunk.getDensity().toString()).getConcurrency(), chunk, isConcurrencyChange, isPipeChange);
                    if (isPipeChange) {
                        changePipeline(chunk);
                    }
                    for (int i = 0; i < chunk.getRecords().channels.size(); i++) {
                        if (!channelInUseForThisChunk.contains(chunk.getRecords().channels.get(i))) {
                            if (ConfigurationParams.parallelismOptimization) {
                                debugLogger.debug("Channel in use effective par: " + chunk.getRecords().channels.get(i).parallelism);
                            }
                            channelInUse.add(chunk.getRecords().channels.get(i));
                            Runnable runs = new RunTransfers(chunk.getRecords().channels.get(i));
                            GridFTPClient.executor.submit(runs);
                        }
                        System.err.println("Channel ID: " + chunk.getRecords().channels.get(i).getId() + "\n pipe = " + chunk.getRecords().channels.get(i).getPipelining());
                    }

                    if (chunk.getRecords().channels.size() < chunk.getTunableParameters().getConcurrency()) {
                        isExtraChannelNeeded = true;
                        extraChCount = chunk.getTunableParameters().getConcurrency() - chunk.getRecords().channels.size();
                    }

                    createExtraChannels(chunk);
                }
            }
            profilingCounter++;
            if (isNewFile) {
                dataNotChangeCounter = 1;
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
        dataSizeofCurrentTransfer += datasetSize;
        LOG.info("file listing completed at:" + ((System.currentTimeMillis() - startTime) / 1000.0) +
                " data size:" + Utils.printSize(datasetSize, true));
        if (dataset.getFileList().size() == 0) {
            System.err.print("No files found. System exit.");
            System.exit(0);
        }
        chunks = Utils.createFileClusters(dataset, tmpchunks, conf.bandwidth, conf.rtt, maximumChunks);
        tmpchunks = chunks;

        int[][] estimatedParamsForChunks = new int[chunks.size()][4];
        gridFTPClient.startTransferMonitor(this);

        // Make sure total channels count does not exceed total file count
//        int totalChannelCount = Math.min(conf.maxConcurrency, dataset.count());
        for (int i = 0; i < estimatedParamsForChunks.length; i++) {
            SessionParameters sp = new SessionParameters();
            TunableParameters tb = Utils.getBestParams(chunks.get(i).getRecords(), maximumChunks);
            chunks.get(i).setTunableParameters(tb);
            sp.setConcurrency(chunks.get(i).getTunableParameters().getConcurrency());
            sp.setParallelism(chunks.get(i).getTunableParameters().getParallelism());
            sp.setPipelining(chunks.get(i).getTunableParameters().getPipelining());
            sp.setBufferSize(chunks.get(i).getTunableParameters().getBufferSize());
            sp.setChunkType(chunks.get(i).getDensity().toString());
            sessionParametersMap.put(chunks.get(i).getDensity().toString(),
                    sessionParametersMap.getOrDefault(chunks.get(i).getDensity().toString(), sp));
            if (!ConfigurationParams.isStaticTransfer) {
                debugLogger.debug("--------------------------------");

                debugLogger.debug("[INITIAL] Concurrency: " + sessionParametersMap.get(chunks.get(i).getDensity().toString()).getConcurrency() +
                        " Parallelism: " + sessionParametersMap.get(chunks.get(i).getDensity().toString()).getParallelism() +
                        " Pipelining: " + sessionParametersMap.get(chunks.get(i).getDensity().toString()).getPipelining());
                if (printSysOut)
                    System.out.println("[INITIAL] Concurrency: " + sessionParametersMap.get(chunks.get(i).getDensity().toString()).getConcurrency() +
                            " Parallelism: " + sessionParametersMap.get(chunks.get(i).getDensity().toString()).getParallelism() +
                            " Pipelining: " + sessionParametersMap.get(chunks.get(i).getDensity().toString()).getPipelining());
            }
        }
        LOG.info(" Running MC with :" + transferTask.getMaxConcurrency() + " channels.");
        for (FileCluster chunk : chunks) {
            LOG.info("Chunk :" + chunk.getDensity().name() + " cc:" + chunk.getTunableParameters().getConcurrency() +
                    " p:" + chunk.getTunableParameters().getParallelism() + " ppq:" + chunk.getTunableParameters().getPipelining());
        }
        if (ConfigurationParams.isStaticTransfer) {
            debugLogger.debug("--------------------------------");

            for (int i = 0; i < estimatedParamsForChunks.length; i++) {
                debugLogger.debug("[INITIAL] confs = " + "Concurrency: " + chunks.get(i).getTunableParameters().getConcurrency() +
                        " Parallelism: " + chunks.get(i).getTunableParameters().getParallelism() +
                        " Pipelining: " + chunks.get(i).getTunableParameters().getPipelining());
                debugLogger.info("[INITIAL] confs = " + "Concurrency: " + chunks.get(i).getTunableParameters().getConcurrency() +
                        " Parallelism: " + chunks.get(i).getTunableParameters().getParallelism() +
                        " Pipelining: " + chunks.get(i).getTunableParameters().getPipelining());
            }
        }
//        Utils.allocateChannelsToChunks(chunks, transferTask.getMaxConcurrency(), conf.channelDistPolicy);
        for (FileCluster fileCluster : chunks) {
            gridFTPClient.runTransfer(fileCluster);
            channelSettingsOfTransferringChunk(fileCluster, sessionParametersMap.get(fileCluster.getDensity().toString()));
        }
        startMonitorThisTransfer();
        firstTransferCompleted = true;
        if (dataNotChangeCounter >= 200) {
            isTransferCompleted = true;
            GridFTPClient.executor.shutdown();
            while (!GridFTPClient.executor.isTerminated()) {
                //wait for
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
        if (monitorThisTransfer != null && monitorThisTransfer.isAlive()) {
            monitorThisTransfer = null;
        }
        debugLogger.debug("Transfer: " + TRANSFER_NUMBER + " completed. Information removing...");
    }

    private void addNewFilesToChunks() throws Exception {
//        historicalFirstCheck = false;
        XferList newFiles = newDataset;
        dataSizeofCurrentTransfer += newFiles.size();
        chunks = Utils.createFileClusters(newFiles, chunks, conf.bandwidth, conf.rtt, maximumChunks);

        for (FileCluster f : chunks) {
            System.err.println("Chunk: " + f.getDensity().toString() + " size: " + f.getRecords().getUnderstandableSize());
        }

        int[][] estimatedParamsForChunks = new int[chunks.size()][4];

        tuneNewChunkParameters(estimatedParamsForChunks);

        LOG.info(" Running MC with :" + transferTask.getMaxConcurrency() + " channels.");
        for (FileCluster chunk : chunks) {
            debugLogger.debug("Chunk:" + chunk.getDensity() + " conc: " + chunk.getTunableParameters().getConcurrency() + " pipe:" +
                    chunk.getTunableParameters().getPipelining() + " paral: " + chunk.getTunableParameters().getParallelism());
        }

        //assign max allowed channel sizes
        // because we have this size of channel now!
        if (ConfigurationParams.isStaticTransfer) {
            transferTask.setMaxConcurrency(GridFTPClient.TransferChannel.channelPairList.size());
        }

        if (!ConfigurationParams.profiling)
            Utils.allocateChannelsToChunks(chunks, transferTask.getMaxConcurrency(), conf.channelDistPolicy);

        if (chunks.size() > 1) {

            if ((chunks.get(0).getRecords().channels == null && chunks.get(1).getRecords().channels != null) ||
                    (chunks.get(1).getRecords().channels != null && chunks.get(0).getRecords().channels != null &&
                            chunks.get(1).getRecords().channels.size() > chunks.get(1).getRecords().channels.size())) {
                System.out.println("CHUNK SWAP NEEDED!");
                Collections.swap(chunks, 0, 1);
                isSwapped = true;
            }
        }
        for (FileCluster chunk : chunks) {
            System.out.println("Chunk " + chunk.getDensity() + ":\tfiles:" + chunk.getRecords().count() + "\t avg:" +
                    Utils.printSize(chunk.getCentroid(), true)
                    + " \t total:" + Utils.printSize(chunk.getRecords().size(), true) + " Density:" +
                    chunk.getDensity());

            if (!GridFTPClient.ftpClient.fileClusters.contains(chunk)) {
                GridFTPClient.ftpClient.fileClusters.add(chunk);
            }
            XferList xl = chunk.getRecords();
            synchronized (chunk.getRecords()) {
                xl.initialSize = xl.initialSize + newDatasetSizes.getOrDefault(chunk.getDensity().toString(), xl.size());
            }

            chunk.isReadyToTransfer = true;

            if (xl.channels == null || xl.channels.size() == 0) {
                xl.channels = new ArrayList<>();
            }

            synchronized (chunk.getRecords()) {
                xl.updateDestinationPaths();
            }

            if (!ConfigurationParams.isStaticTransfer) {
                long strt = System.currentTimeMillis();
                String cType = chunk.getDensity().toString();
                boolean isConcurrencyChange = isConcChange(sessionParametersMap.get(cType), xl.channels, conf.maxConcurrency);
                boolean isPipeChange = isPipChanged(sessionParametersMap.get(cType), xl.channels);
                boolean isChangeNeed = isConcurrencyChange || isPipeChange;
                if (!isChangeNeed) {
                    if (printSysOut)
                        System.err.println("NO NEED CHANGE FOR: " + chunk.getDensity().toString() +
                                " CHUNK CHANGE. IT WILL TRANSFER WITH CURRENT SETTINGS.");
                    debugLogger.info("NO NEED CHANGE FOR: " + chunk.getDensity().toString() +
                            " CHUNK CHANGE. IT WILL TRANSFER WITH CURRENT SETTINGS.");
                    debugLogger.info("Current setting remain for " + chunk.getDensity().toString() + " chunk. Continue without change..");
                    channelSettingsOfTransferringChunk(chunk, sessionParametersMap.get(cType));
                    continue;
                }
                HashSet<ChannelModule.ChannelPair> channelInUseForThisChunk = new HashSet<>(chunk.getRecords().channels);
                if (!isConcurrencyChange) {
                    changePipeline(chunk);
                    continue;
                } else {
                    allocateChannelsOnDemand(chunk.getTunableParameters().getConcurrency(),
                            sessionParametersMap.get(cType).getConcurrency(), chunk, isConcurrencyChange, isPipeChange);
                }
                if (isPipeChange) {
                    changePipeline(chunk);
                }
                System.out.println("Transfer started = " + chunk.getDensity().toString() + " need for = " +
                        sessionParametersMap.get(cType).getConcurrency()
                        + " allowed max = " + chunk.getTunableParameters().getConcurrency() + " allocated = " +
                        xl.channels.size());
                debugLogger.info("Transfer started = " + chunk.getDensity().toString() + " need for = " +
                        sessionParametersMap.get(cType).getConcurrency()
                        + " allowed max = " + chunk.getTunableParameters().getConcurrency() + " allocated = " +
                        xl.channels.size());
                channelSettingsOfTransferringChunk(chunk, sessionParametersMap.get(cType));
                long end = 0;
                end += (System.currentTimeMillis() - strt) / 1000;
                LOG.info("Ch allocated in " + end + " seconds.");
                reRunTransferWithNewChannel(xl, channelInUseForThisChunk);
                createExtraChannels(chunk);
            } else {
                HashSet<ChannelModule.ChannelPair> channelInUseForThisChunk = new HashSet<>(chunk.getRecords().channels);
                allocateChannelsOnDemand(chunk.getTunableParameters().getConcurrency(),
                        chunk.getTunableParameters().getConcurrency(), chunk, false, false);
                reRunTransferWithNewChannel(xl, channelInUseForThisChunk);

            }
        }

    }

    private void reRunTransferWithNewChannel(XferList xl, HashSet<ChannelModule.ChannelPair> channelInUseForThisChunk) {
        for (int i = 0; i < xl.channels.size(); i++) {
            if (!channelInUseForThisChunk.contains(xl.channels.get(i))) {
                if (ConfigurationParams.parallelismOptimization) {
                    debugLogger.debug("Channel in use effective par: " + xl.channels.get(i).parallelism);
                }
                channelInUse.add(xl.channels.get(i));
                Runnable runs = new RunTransfers(xl.channels.get(i));
                GridFTPClient.executor.submit(runs);
            }
        }
        startMonitorThisTransfer();
    }

    private void createExtraChannels(FileCluster chunk) throws InterruptedException {
        if (extraChCount > conf.maxConcurrency) {
            extraChCount = conf.maxConcurrency - GridFTPClient.TransferChannel.channelPairList.size();
        }
        if (isExtraChannelNeeded) {
            if (printSysOut)
                System.out.println("Need EXTRA CHANNEL COUNT " + extraChCount);
            debugLogger.info("Need EXTRA CHANNEL COUNT " + extraChCount);
            XferList fileList = chunk.getRecords();

            List<XferList.MlsxEntry> firstFilesToSend = Lists.newArrayListWithCapacity(extraChCount);
            for (int i = 0; i < extraChCount; i++) {
                XferList.MlsxEntry e = fileList.pop();
                firstFilesToSend.add(e);
            }

            for (int i = 0; i < extraChCount; i++) {
                XferList.MlsxEntry firstFile = synchronizedPop(firstFilesToSend);
                Runnable transferChannel = new GridFTPClient.TransferChannel(chunk, GridFTPClient.uniqueChannelID, firstFile);
                GridFTPClient.executor.submit(transferChannel);
                GridFTPClient.uniqueChannelID++;
            }


            long s3 = System.currentTimeMillis();
            long e3 = 0;
            while (GridFTPClient.channelCreationStarted) {
                Thread.sleep(2);
                //wait.
            }
            e3 += (System.currentTimeMillis() - s3) / 1000;
            if (printSysOut)
                System.out.println("Extra channels created in :" + e3 + " secs.");
            debugLogger.info("Channel created in :" + e3 + " secs.");
            isExtraChannelNeeded = false;
        }
    }

    private void stopChannelsandMarkedAvailable(int stopCount, FileCluster chunk) {
        if (printSysOut)
            System.err.println("Stop channels.");
        debugLogger.info("Stop channels.");
        ArrayList<ChannelModule.ChannelPair> list = new ArrayList<>();
        for (int i = 0; i < stopCount; i++) {
            if (printSysOut)
                System.out.println("Channel = " + chunk.getRecords().channels.get(i) + " going to mark as remove..");
            debugLogger.info("Channel = " + chunk.getRecords().channels.get(i) + " going to mark as remove..");
            synchronized (chunk.getRecords().channels.get(i)) {
                if (printSysOut)
                    System.out.println("[SYNC] Channel = " + chunk.getRecords().channels.get(i) + " going to mark as remove..");
                debugLogger.info("[SYNC] Channel = " + chunk.getRecords().channels.get(i) + " going to mark as remove..");
                chunk.getRecords().channels.get(i).setMarkedAsRemove(true);
                list.add(chunk.getRecords().channels.get(i));
            }
        }
        if (printSysOut)
            System.out.println("All channels marked as remove.");
        debugLogger.info("All channels marked as remove.");
        while (list.size() != 0) {
            try {
                Thread.sleep(1); // give a chance to breath
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (list.get(0).inTransitFiles.size() == 0 && !channelInUse.contains(list.get(0))) {
                list.remove(0);
            }
        }
        if (printSysOut)
            System.out.println(stopCount + " size channels stopped");
        debugLogger.info(stopCount + " size channels stopped");
    }

    private void startMonitorThisTransfer() {
        if (monitorThisTransfer == null || !monitorThisTransfer.isAlive()) {
            if (printSysOut)
                System.out.println("Start monitoring this transfer...");
            debugLogger.info("Start monitoring this transfer...");
            monitorThisTransfer = new MonitorTransfer(this, chunks);
            monitorThisTransfer.start();
        } else {
            if (printSysOut)
                System.out.println("Tracking this transfer is still alive...");
            debugLogger.info("Tracking this transfer is still alive...");
        }
    }


    private void tuneNewChunkParameters(int[][] estimatedParamsForChunks) {
        if (ConfigurationParams.isStaticTransfer) {
            for (int i = 0; i < estimatedParamsForChunks.length; i++) {
                String cType = chunks.get(i).getDensity().toString();
                if (sessionParametersMap.containsKey(cType)) {
                    TunableParameters tb = new TunableParameters(sessionParametersMap.get(cType).getConcurrency(),
                            sessionParametersMap.get(cType).getParallelism(), sessionParametersMap.get(cType).getPipelining(),
                            sessionParametersMap.get(cType).getBufferSize());
                    chunks.get(i).setTunableParameters(tb);
                } else {
                    debugLogger.debug("There no initial settings for " + chunks.get(i).getDensity()
                            + " size chunks. Using static one...");
                    SessionParameters session = null;
                    for (SessionParameters s : sessionParametersMap.values()) {
                        session = s;
                    }
                    if (session == null) {
                        System.err.println("No exist Session : FATAL");
                    }
                    TunableParameters tunableParameters = new TunableParameters(session.getConcurrency(),
                            session.getParallelism(), session.getPipelining(),
                            session.getBufferSize());
                    SessionParameters sp = new SessionParameters();
                    sp.setPipelining(tunableParameters.getPipelining());
                    sp.setConcurrency(tunableParameters.getConcurrency());
                    sp.setParallelism(tunableParameters.getParallelism());
                    sp.setChunkType(chunks.get(i).getDensity().toString());
                    sessionParametersMap.put(chunks.get(i).getDensity().toString(), sp);
                    chunks.get(i).setTunableParameters(tunableParameters);
                }

                debugLogger.debug("[STATIC] Chunk: = " + chunks.get(i).getDensity() + "confs: Concurrency: "
                        + chunks.get(i).getTunableParameters().getConcurrency()
                        + " Parallelism: " + chunks.get(i).getTunableParameters().getParallelism()
                        + " Pipelining: " + chunks.get(i).getTunableParameters().getPipelining() +
                        "; Updated file count: " + chunks.get(i).getRecords().getFileList().size() +
                        "; Chunk Size: " + Utils.printSize(chunks.get(i).getTotalSize(), true));
            }
        } else {

            HashMap<String, TunableParameters> tunableParametersHashMap = new HashMap<>();
            for (FileCluster chunk : chunks) {
                tunableParametersHashMap.put(chunk.getDensity().toString(), Utils.getBestParams(chunk.getRecords(), maximumChunks));
            }
            for (int i = 0; i < estimatedParamsForChunks.length; i++) {
                String cType = chunks.get(i).getDensity().toString();
                TunableParameters tb = tunableParametersHashMap.get(cType);
                if (sessionParametersMap.containsKey(cType)) { //we have both
                    SessionParameters sp = sessionParametersMap.get(cType);

                    debugLogger.debug("[SESSION] Chunk " + chunks.get(i).getDensity()
                            + "[CUR] concurrency = " + sp.getConcurrency()
                            + "; parallelism = " + sp.getParallelism()
                            + "; pipelining = " + sp.getPipelining());
                    if (!ConfigurationParams.profiling || (!historicalProfiling.containsKey(cType) &&
                            (chunks.get(i).getTunableParameters() == null || chunks.get(i).getTunableParameters().getConcurrency() == 0))) {
                        sp.setConcurrency(tb.getConcurrency());
                    } else if (ConfigurationParams.profiling && ConfigurationParams.history && historicalProfiling.containsKey(cType)) {
                        System.err.println("HISTORY--------- Historical check Completed CONCURRENCY = " + historicalProfiling.get(cType));
                        sp.setConcurrency(historicalProfiling.get(cType));
                        tb.setConcurrency(historicalProfiling.get(cType));
                    }
                    sp.setPipelining(tb.getPipelining());
                    if (!ConfigurationParams.profiling) {
                        sp.setParallelism(tb.getParallelism());
                    }
                    sp.setBufferSize(tb.getBufferSize());

                    runChannelsWithNewConf(i, cType, tb, sp);
                } else {
                    SessionParameters sp = new SessionParameters();
                    if (!ConfigurationParams.profiling || (!historicalProfiling.containsKey(cType) &&
                            (chunks.get(i).getTunableParameters() == null || chunks.get(i).getTunableParameters().getConcurrency() == 0))) {
                        sp.setConcurrency(tb.getConcurrency());
                    } else if (ConfigurationParams.profiling && ConfigurationParams.history && historicalProfiling.containsKey(cType)) {
                        System.err.println("HISTORY-2-2--------- Historical check Completed CONCURRENCY = " + historicalProfiling.get(cType));
                        sp.setConcurrency(historicalProfiling.get(cType));
                        tb.setConcurrency(historicalProfiling.get(cType));
                    }
                    sp.setPipelining(tb.getPipelining());
                    sp.setParallelism(tb.getParallelism());
                    sp.setBufferSize(tb.getBufferSize());
                    sessionParametersMap.put(cType, sp);
                    runChannelsWithNewConf(i, cType, tb, sp);
                }
                debugLogger.debug("[DYNAMIC] Chunk: " + chunks.get(i).getDensity() + "; [REQ] Concurrency: " +
                        chunks.get(i).getTunableParameters().getConcurrency() +
                        "; Parallelism: " + chunks.get(i).getTunableParameters().getParallelism() +
                        "; Pipelining: " + chunks.get(i).getTunableParameters().getPipelining() +
                        "; Updated file count: " + chunks.get(i).getRecords().getFileList().size() +
                        "; Chunk Size: " + Utils.printSize(chunks.get(i).getTotalSize(), true));
                if (printSysOut)
                    System.out.println("[DYNAMIC] Chunk: " + chunks.get(i).getDensity() + "; [REQ] Concurrency: " + chunks.get(i).getTunableParameters().getConcurrency() +
                            "; Parallelism: " + chunks.get(i).getTunableParameters().getParallelism() +
                            "; Pipelining: " + chunks.get(i).getTunableParameters().getPipelining() +
                            "; Updated file count = " + chunks.get(i).getRecords().getFileList().size() +
                            "; Chunk Size: " + Utils.printSize(chunks.get(i).getTotalSize(), true));
            }
        }
    }

    private void runChannelsWithNewConf(int i, String cType, TunableParameters tb, SessionParameters sp) {
        if (!ConfigurationParams.isStaticTransfer && !ConfigurationParams.profiling &&
                (cType.equals("LARGE") || (cType.equals("SMALL"))) && tb.getConcurrency() <= 2) {
            tb.setConcurrency(tb.getConcurrency() * 2);
            chunks.get(i).setTunableParameters(tb);
            sp.setConcurrency(tb.getConcurrency());
        } else {
            chunks.get(i).setTunableParameters(tb);
        }
    }

    private void allocateChannelsOnDemand(int maxConcForThisChunk, int desiredConcurrency, FileCluster chunk,
                                          boolean isConcurrencyChange, boolean isPipeChange) throws InterruptedException {

        if (ConfigurationParams.isStaticTransfer && chunks.size() >= 2) {
            if (chunk.getRecords().channels.size() < desiredConcurrency && chunks.get(1).getRecords().channels.size() > 0) {
                FileCluster otherChunk = chunks.get(1);
                int stopChannelCount = desiredConcurrency - chunk.getRecords().channels.size();
                stopChannelsandMarkedAvailable(stopChannelCount, otherChunk);
            }
        }

        if (printSysOut)
            System.err.println("----------CHANNEL DISTRIBUTION ON DEMAND STARTED... ");
        LOG.info("----------CHANNEL DISTRIBUTION ON DEMAND STARTED... ");

        String chunkType = chunk.getDensity().toString();
        if (printSysOut)
            System.out.println("Chunk Type = " + chunkType + " IN NEED:"
                    + desiredConcurrency + "; Current channel size:" + chunk.getRecords().channels.size()
                    + "; MAX ALLOWED : " + maxConcForThisChunk + " available channel count = " + GridFTPClient.TransferChannel.channelPairList.size());

        LOG.info("Chunk Type = " + chunkType + " IN NEED:"
                + desiredConcurrency + "; Current channel size:" + chunk.getRecords().channels.size()
                + "; MAX ALLOWED : " + maxConcForThisChunk + " available channel count = " + GridFTPClient.TransferChannel.channelPairList.size());
        if (desiredConcurrency == chunk.getRecords().channels.size()) {
            return;
        }
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
            if (printSysOut)
                System.out.println("Current channel size is bigger than we need. Reducing count of " +
                        (chunk.getRecords().channels.size() - desiredConcurrency) + " channels.");
            LOG.info("Current channel size is bigger than we need. Reducing count of " +
                    (chunk.getRecords().channels.size() - desiredConcurrency) + " channels.");
            ArrayList<ChannelModule.ChannelPair> list = new ArrayList<>();
            for (int i = 0; i < chunk.getRecords().channels.size() - desiredConcurrency; i++) {
                ChannelModule.ChannelPair channel = chunk.getRecords().channels.get(i);
                if (printSysOut)
                    System.out.println("Channel ID = " + channel.getId() + " going to mark as remove..");
                LOG.info("Channel ID = " + channel.getId() + " going to mark as remove..");

                synchronized (chunk.getRecords().channels.get(i)) {
                    if (printSysOut)
                        System.out.println("[SYNC] Channel ID = " + channel.getId() + " going to mark as remove..");
                    LOG.info("[SYNC] Channel ID = " + channel.getId() + " going to mark as remove..");
                    chunk.getRecords().channels.get(i).setMarkedAsRemove(true);
                    while (channel.inTransitFiles.size() != 0 || channelInUse.contains(channel)) {
                        Thread.sleep(10);
                    }
                    chunk.getRecords().channels.remove(channel);
                    System.out.println("Channel ID = " + channel.getId() + " removed from this chunk.");
                    list.add(channel);
                }
            }

            Thread.sleep(50);
            if (printSysOut)
                System.out.println("Channels decreased to size = " + chunk.getRecords().channels.size());
            LOG.info("Channels decreased to size = " + chunk.getRecords().channels.size());

            if (isPipeChange) {
                for (int i = 0; i < chunk.getRecords().channels.size(); i++) {
                    if (chunk.getRecords().channels.get(i).getPipelining() != chunk.getTunableParameters().getPipelining()) {
                        chunk.getRecords().channels.get(i).setPipelining(chunk.getTunableParameters().getPipelining());
                    }
                }
            }
            return;
        }

        // that mean we have some channels in use for this chunk
        // so need to figure out how many new required
        if (chunk.getRecords().channels.size() != 0 && desiredConcurrency > chunk.getRecords().channels.size()) {
            desiredConcurrency = desiredConcurrency - chunk.getRecords().channels.size();
        }
        LOG.info("Mid path, desiredCond = " + desiredConcurrency);
        int availableChannelCount = GridFTPClient.TransferChannel.channelPairList.size() - channelInUse.size();
        if (printSysOut)
            System.out.println("Desired: " + desiredConcurrency +
                    "; ChannelCount: " + GridFTPClient.TransferChannel.channelPairList.size() +
                    "; channelInUse: " + channelInUse.size() +
                    "; available: " + availableChannelCount);
        LOG.info("Desired: " + desiredConcurrency +
                "; ChannelCount: " + GridFTPClient.TransferChannel.channelPairList.size() +
                "; channelInUse: " + channelInUse.size() +
                "; available: " + availableChannelCount);

        if (desiredConcurrency != chunk.getRecords().channels.size() && desiredConcurrency > availableChannelCount) {
            if (printSysOut)
                System.out.println("desired conc is bigger than available channel count will create new channels after transfer start....");
            LOG.info("desired conc is bigger than available channel count will create new channels after transfer start....");
            LOG.info("desiredConcurrency = " + desiredConcurrency + "; chunk.getRecords().channels.size()" + chunk.getRecords().channels.size() +
                    "; availableChannelCount = " + availableChannelCount);
            isExtraChannelNeeded = true;
            extraChCount = desiredConcurrency - availableChannelCount;
            desiredConcurrency = availableChannelCount;
            if (desiredConcurrency == 0) return;
        }

        // if we come so far, we need to add new channels. so pick channels from available sets.
        // if there is not enough available channels then satisfy as much as we can.
        if (printSysOut)
            System.out.println("Need abs channel count = " + desiredConcurrency);
        LOG.info("Need abs channel count = " + desiredConcurrency + " current ch size for this chunk: " + chunk.getRecords().channels.size());

        int index = 0;
        long start = System.currentTimeMillis();
        long timeSpent = 0;
        int desiredCounter = 0;
        int reqParallelism = chunk.getTunableParameters().getParallelism();
        int alreadyHasThisParalCount = 0;
        while (!channelsDistributed) {
            // first of all, changing parallelism is costly so look that already changed one
            // and if it available then assign
            if (channelsWithParallelismCountMap.containsKey(reqParallelism)) {
                ArrayList<ChannelModule.ChannelPair> channels = channelsWithParallelismCountMap.get(reqParallelism);
                for (ChannelModule.ChannelPair cc : channels) {
                    if (index == maxConcForThisChunk || index == desiredConcurrency) {
                        channelsDistributed = true;
                        break;
                    }
                    if (channelsDemanded != null && channelsDemanded.containsKey(cc) && !channelsDemanded.get(cc)) {
                        LOG.info("Channel " + cc.getId() + " assigned");
                        cc.chunk = chunk;
                        chunk.getRecords().channels.add(cc);
                        channelsDemanded.put(cc, true);
                        alreadyHasThisParalCount++;
                        index++;
                    } else if (otherChannels != null && otherChannels.containsKey(cc) && !otherChannels.get(cc)) {
                        LOG.info("Channel " + cc.getId() + " assigned");
                        cc.chunk = chunk;
                        chunk.getRecords().channels.add(cc);
                        otherChannels.remove(cc);
                        channelsDemanded.put(cc, true);
                        alreadyHasThisParalCount++;
                        index++;
                    }

                }
            }
            boolean needOtherChannels = false;
            while (index < maxConcForThisChunk && index < desiredConcurrency && !needOtherChannels) {
                if (channelsDemanded != null) {
                    for (ChannelModule.ChannelPair cp : channelsDemanded.keySet()) {
                        if (!channelsDemanded.get(cp)) {
                            LOG.info("Channel " + cp.getId() + " assigned");
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
                        if (printSysOut)
                            System.err.println("Looking for " + (desiredConcurrency - index) + " more channels");
                        LOG.info("Looking for " + (desiredConcurrency - index) + " more channels");
                        needOtherChannels = true;
                    } else {
                        channelsDistributed = true;
                    }

                }
            }
            desiredCounter++;
            if (desiredCounter == 500) {
                if (printSysOut)
                    System.err.println("Channel allocation break with distribution =  " + channelsDistributed);
                LOG.info("Channel allocation break with distribution =  " + channelsDistributed);
                break;
            }
            if (needOtherChannels) {
                if (otherChannels != null) {
                    for (ChannelModule.ChannelPair cp : otherChannels.keySet()) {
                        if (!otherChannels.get(cp)) {
                            LOG.info("Channel " + cp.getId() + " assigned");
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
        if (printSysOut)
            System.err.println(alreadyHasThisParalCount + " channels added from same parallelism set channels.");
        LOG.info(alreadyHasThisParalCount + " channels added from same parallelism set channels.");


        timeSpent += ((System.currentTimeMillis() - start) / 1000.0);
        if (printSysOut)
            System.err.println(chunk.getRecords().channels.size() + " channel for " + chunkType + " chunk allocated." +
                    " Time: " + timeSpent);
        LOG.info(chunk.getRecords().channels.size() + " channel for " + chunkType + " chunk allocated." +
                " Time: " + timeSpent);
        debugLogger.debug("MAX ALLOWEd: " + maxConcForThisChunk + " TOTAL ALLOCATED: " + chunk.getRecords().channels.size());
    }

    private void changePipeline(FileCluster chunk) {
        for (ChannelModule.ChannelPair c : chunk.getRecords().channels) {
//            System.out.println("Cur pipie: " + c.getPipelining() );
            if (c.getPipelining() != chunk.getTunableParameters().getPipelining()) {
                synchronized (c) {
                    c.setPipelining(chunk.getTunableParameters().getPipelining());
                }
            }
//            System.out.println("After pipe: " + c.getPipelining());
        }
    }

    private void channelSettingsOfTransferringChunk(FileCluster chunk, SessionParameters sessionParameters) throws IOException {
        System.err.println("Requested settings: paral" + sessionParameters.getParallelism() + "; pipe: " + sessionParameters.getPipelining());
        LOG.info("Requested settings: paral" + sessionParameters.getParallelism() + "; pipe: " + sessionParameters.getPipelining());
        System.out.println("CHANNEL SETTINGS BEFORE TRANSFER: ");
        LOG.info("CHANNEL SETTINGS BEFORE TRANSFER: ");
        int i = 0;
        int p = 0;
        int pp = 0;
        for (ChannelModule.ChannelPair c : chunk.getRecords().channels) {
            System.out.println("Channel " + c.getId() + "; paral: " + c.parallelism + "; pipe: " + c.getPipelining());
            LOG.info("Channel " + c.getId() + "; paral: " + c.parallelism + "; pipe: " + c.getPipelining());
            i++;
            p = c.parallelism;
            pp = c.getPipelining();
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
            if (printSysOut)
                System.out.println("Pipelining change need because: old = " + curPipelining + " new = " + requestedPipelining);
            LOG.info("Pipelining change need because: old = " + curPipelining + " new = " + requestedPipelining);
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
            if (printSysOut)
                System.out.println("Concurrency change need because: old = " + currentConc + " new = " + requestedNewConcurrency);
            LOG.info("Concurrency change need because: old = " + currentConc + " new = " + requestedNewConcurrency);
            return true;
        }
        return false;
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

    private XferList.MlsxEntry synchronizedPop(List<XferList.MlsxEntry> fileList) {
        synchronized (fileList) {
            return fileList.remove(0);
        }
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
}
