package client;

import client.hysterisis.Entry;
import client.hysterisis.Hysteresis;
import client.utils.MonitorTransfer;
import client.utils.RunTransfers;
import client.utils.TunableParameters;
import client.utils.Utils;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;

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
    //    private List<SessionParameters> sessionParameters = new ArrayList<>();
    public static HashMap<String, SessionParameters> sessionParametersMap = new HashMap<>();

    public static HashMap<Integer, Boolean> isTransfersCopmletedMap = new HashMap<>();

    private final static Logger debugLogger = LogManager.getLogger("reportsLogger");

    public static ConcurrentHashMap<ChannelModule.ChannelPair, Boolean> smallMarkedChannels = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<ChannelModule.ChannelPair, Boolean> largeMarkedChannels = new ConcurrentHashMap<>();

    public static HashSet<ChannelModule.ChannelPair> channelInUse = new HashSet<>();
    public static HashMap<Integer, ArrayList<ChannelModule.ChannelPair>> channelsWithParallelismCountMap = new HashMap<>();

    private Thread monitorThisTransfer;
    public static long dataSizeofCurrentTransfer = 0;

    private boolean isExtraChannelNeeded = false;
    private int extraChCount = 0;

    private boolean printSysOut = true;

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
//
//
//        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
//        executor.scheduleAtFixedRate(() -> {
//            try {
//                adaptiveGridFTPClient.checkNewData();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }, 0, 10, TimeUnit.SECONDS);

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
            if (gridFTPClient == null) {
                System.out.println("Client is null.");
            }

            dataset = gridFTPClient.getListofFiles(allFiles);
            //if there is data then cont.
            isNewFile = false;
            System.out.println("[BEFORE] dataset.size = " + dataset.getFileList().size());
            System.out.println("[BEFORE] allFiles.size = " + allFiles.size());
            // this can be time consuming in case of large number of files
            for (int i = 0; i < dataset.getFileList().size(); i++) {
                if (allFiles.contains(dataset.getFileList().get(i).fullPath())) {
                    dataset.removeItem(i);
                    i -=1;
                }else{
                    allFiles.add(dataset.getFileList().get(i).fullPath());
                    isNewFile = true;
                }
            }

            System.out.println("[AFTER] dataset.size = " + dataset.getFileList().size());
            System.out.println("[AFTER] allFiles.size = " + allFiles.size());
            System.out.println("[AFTER] isNewFile = " + isNewFile);

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
            LOG.info("Checking data counter = " + dataNotChangeCounter);
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
        gridFTPClient.startTransferMonitor(this);

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
                sessionParametersMap.put(chunks.get(i).getDensity().toString(),
                        sessionParametersMap.getOrDefault(chunks.get(i).getDensity().toString(), sp));
                if (!ConfigurationParams.isStaticTransfer) {
                    debugLogger.debug("[INITIAL] Concurrency: " + sessionParametersMap.get(chunks.get(i).getDensity().toString()).getConcurrency() +
                            " Parallelism: " + sessionParametersMap.get(chunks.get(i).getDensity().toString()).getParallelism() +
                            " Pipelining: " + sessionParametersMap.get(chunks.get(i).getDensity().toString()).getPipelining());
                    LOG.info("[INITIAL] Concurrency: " + sessionParametersMap.get(chunks.get(i).getDensity().toString()).getConcurrency() +
                            " Parallelism: " + sessionParametersMap.get(chunks.get(i).getDensity().toString()).getParallelism() +
                            " Pipelining: " + sessionParametersMap.get(chunks.get(i).getDensity().toString()).getPipelining());
                    if (printSysOut)
                        System.out.println("[INITIAL] Concurrency: " + sessionParametersMap.get(chunks.get(i).getDensity().toString()).getConcurrency() +
                                " Parallelism: " + sessionParametersMap.get(chunks.get(i).getDensity().toString()).getParallelism() +
                                " Pipelining: " + sessionParametersMap.get(chunks.get(i).getDensity().toString()).getPipelining());
                }
            }
        }

        LOG.info(" Running MC with :" + transferTask.getMaxConcurrency() + " channels.");

        for (FileCluster chunk : chunks) {
            LOG.info("Chunk :" + chunk.getDensity().name() + " cc:" + chunk.getTunableParameters().getConcurrency() +
                    " p:" + chunk.getTunableParameters().getParallelism() + " ppq:" + chunk.getTunableParameters().getPipelining());
        }

//        Utils.allocateChannelsToChunks(chunks, transferTask.getMaxConcurrency(), conf.channelDistPolicy);

        if (ConfigurationParams.isStaticTransfer) {
            for (int i = 0; i < estimatedParamsForChunks.length; i++) {
                debugLogger.debug("[INITIAL] confs = " + "Concurrency: " + chunks.get(i).getTunableParameters().getConcurrency() +
                        " Parallelism: " + chunks.get(i).getTunableParameters().getParallelism() +
                        " Pipelining: " + chunks.get(i).getTunableParameters().getPipelining());
                LOG.info("[INITIAL] confs = " + "Concurrency: " + chunks.get(i).getTunableParameters().getConcurrency() +
                        " Parallelism: " + chunks.get(i).getTunableParameters().getParallelism() +
                        " Pipelining: " + chunks.get(i).getTunableParameters().getPipelining());
            }

        }


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
        LOG.info(conf.algorithm.name() +
                " fileClusters: " + maximumChunks +
                " size:" + Utils.printSize(datasetSize, true) +
                " time:" + timeSpent +
                " thr: " + (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)));
       LOG.info("Current available channel: " + GridFTPClient.TransferChannel.channelPairList.size());
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
        if (monitorThisTransfer != null && monitorThisTransfer.isAlive()) {
            monitorThisTransfer = null;
        }
        debugLogger.debug("Transfer: " + TRANSFER_NUMBER + " completed. Information removing...");
    }

    private void addNewFilesToChunks() throws Exception {

        XferList newFiles = newDataset;
        dataSizeofCurrentTransfer += newFiles.size();
        chunks = Utils.createFileClusters(newFiles, chunks, conf.bandwidth, conf.rtt, maximumChunks);
        int[][] estimatedParamsForChunks = new int[chunks.size()][4];

        tuneNewChunkParameters(estimatedParamsForChunks);

        LOG.info(" Running MC with :" + transferTask.getMaxConcurrency() + " channels.");
        for (FileCluster chunk : chunks) {
            LOG.info("Chunk :" + chunk.getDensity().name() + " cc:" + chunk.getTunableParameters().getConcurrency() +
                    " p:" + chunk.getTunableParameters().getParallelism() + " ppq:" + chunk.getTunableParameters().getPipelining());
        }

        //assign max allowed channel sizes
        if (!ConfigurationParams.isStaticTransfer) {
            Utils.allocateChannelsToChunks(chunks, transferTask.getMaxConcurrency(), conf.channelDistPolicy);
        }
        int staticChannelIndex = 0;
        for (FileCluster chunk : chunks) {

//            System.out.println("New files transferring... Chunk = " + chunk.getDensity());
            LOG.info("New files transferring... Chunk = " + chunk.getDensity());

            if (!GridFTPClient.ftpClient.fileClusters.contains(chunk)) {
                GridFTPClient.ftpClient.fileClusters.add(chunk);
            }

            XferList xl = chunk.getRecords();
            xl.initialSize += xl.size() - xl.totalTransferredSize;
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
                boolean isConcurrencyChange = isConcChange(sessionParametersMap.get(cType), xl.channels, chunk.getTunableParameters().getConcurrency());
                boolean isPipeChange = isPipChanged(sessionParametersMap.get(cType), xl.channels);
                boolean isChangeNeed = isConcurrencyChange || isPipeChange;
                if (!isChangeNeed) {
                    if (printSysOut)
                        System.err.println("NO NEED CHANGE FOR: " + chunk.getDensity().toString() +
                                " CHUNK CHANGE. IT WILL TRANSFER WITH CURRENT SETTINGS.");
                    LOG.info("NO NEED CHANGE FOR: " + chunk.getDensity().toString() +
                            " CHUNK CHANGE. IT WILL TRANSFER WITH CURRENT SETTINGS.");
                    LOG.info("Current setting remain for " + chunk.getDensity().toString() + " chunk. Continue without change..");
                    continue;
                }
                HashSet<ChannelModule.ChannelPair> channelInUseForThisChunk = new HashSet<>(chunk.getRecords().channels);
//                System.out.println("channelInUseForThisChunk size = " + channelInUseForThisChunk.size());

                allocateChannelsOnDemand(chunk.getTunableParameters().getConcurrency(),
                        sessionParametersMap.get(cType).getConcurrency(), chunk, isConcurrencyChange, isPipeChange);


//                LOG.info("Transfer chunk type = " + chunk.getDensity().toString() +
//                        " session params type = " + sessionParametersMap.get(cType).getChunkType());
//                System.out.println("Transfer chunk type = " + chunk.getDensity().toString() +
//                        " session params type = " + sessionParametersMap.get(cType).getChunkType());
                System.out.println("Transfer started = " + chunk.getDensity().toString() + " need for = " +
                        sessionParametersMap.get(cType).getConcurrency()
                        + " allowed max = " + chunk.getTunableParameters().getConcurrency() + " allocated = " +
                        xl.channels.size());
                LOG.info("Transfer started = " + chunk.getDensity().toString() + " need for = " +
                        sessionParametersMap.get(cType).getConcurrency()
                        + " allowed max = " + chunk.getTunableParameters().getConcurrency() + " allocated = " +
                        xl.channels.size());

                channelSettingsOfTransferringChunk(chunk, sessionParametersMap.get(cType));

                long end = 0;
                end += (System.currentTimeMillis() - strt) / 1000;
                LOG.info("Ch allocated in " + end + " seconds.");
                for (int i = 0; i < xl.channels.size(); i++) {
                    if (!channelInUseForThisChunk.contains(xl.channels.get(i))) {
                        channelInUse.add(xl.channels.get(i));
                        Runnable runs = new RunTransfers(xl.channels.get(i));
                        GridFTPClient.executor.submit(runs);
                    }
                }
                if (monitorThisTransfer == null || !monitorThisTransfer.isAlive()) {
                    if (printSysOut)
                        System.out.println("Start monitoring this transfer...");
                    LOG.info("Start monitoring this transfer...");
                    monitorThisTransfer = new MonitorTransfer(this, this.chunks);
                    monitorThisTransfer.start();
                } else {
                    if (printSysOut)
                        System.out.println("Tracking this transfer is still alive...");
                    LOG.info("Tracking this transfer is still alive...");
                }

                boolean isParallelismChange = parallelismChange(sessionParametersMap.get(cType), chunk);
                long start2 = System.currentTimeMillis();
                if (isParallelismChange) {
                    long end2 = 0;
                    end2 += (System.currentTimeMillis() - start2) / 1000;
                    LOG.info("Ch count = " + chunk.getRecords().channels.size() + " parallelism settings time = " + end2 + " seconds.");
                    if (printSysOut)
                        System.out.println("Ch count = " + chunk.getRecords().channels.size() + " parallelism settings time = " + end2 + " seconds.");
                }

                if (isExtraChannelNeeded) {
                    if (printSysOut)
                        System.out.println("Need EXTRA CHANNEL COUNT " + extraChCount);
                    LOG.info("Need EXTRA CHANNEL COUNT " + extraChCount);
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
                    LOG.info("Channel created in :" + e3 + " secs.");
                    isExtraChannelNeeded = false;
                }


            } else {
                //static settings
                for (int i = 0; i < chunk.getTunableParameters().getConcurrency(); i++) {
                    GridFTPClient.TransferChannel.channelPairList.get(staticChannelIndex).chunk = chunk;
                    xl.channels.add(GridFTPClient.TransferChannel.channelPairList.get(staticChannelIndex));
                    staticChannelIndex++;
                }

                for (int i = 0; i < xl.channels.size(); i++) {
                    channelInUse.add(xl.channels.get(i));
                    Runnable runs = new RunTransfers(xl.channels.get(i));
                    GridFTPClient.executor.submit(runs);
                }
                if (monitorThisTransfer == null || !monitorThisTransfer.isAlive()) {
                    if (printSysOut)
                        System.out.println("Start monitoring this transfer...");
                    LOG.info("Start monitoring this transfer...");
                    monitorThisTransfer = new MonitorTransfer(this, this.chunks);
                    monitorThisTransfer.start();
                } else {
                    if (printSysOut)
                        System.out.println("Tracking this transfer is still alive...");
                    LOG.info("Tracking this transfer is still alive...");
                }

            }
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
                LOG.info("[STATIC] Chunk: = " + chunks.get(i).getDensity() + "confs: Concurrency: "
                        + chunks.get(i).getTunableParameters().getConcurrency()
                        + " Parallelism: " + chunks.get(i).getTunableParameters().getParallelism()
                        + " Pipelining: " + chunks.get(i).getTunableParameters().getPipelining() +
                        "; Updated file count: " + chunks.get(i).getRecords().getFileList().size() +
                        "; Chunk Size: " + Utils.printSize(chunks.get(i).getTotalSize(), true));
            }
        } else {

            HashMap<String, TunableParameters> tunableParametersHashMap = new HashMap<>();
//            TunableParameters[] tunableParameters = new TunableParameters[chunks.size()];
            int index = 0;
            for (FileCluster chunk : chunks) {
//                tunableParameters[index] = Utils.getBestParams(chunk.getRecords(), maximumChunks);
                tunableParametersHashMap.put(chunk.getDensity().toString(), Utils.getBestParams(chunk.getRecords(), maximumChunks));
//                index++;
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

                    System.out.println("[SESSION] Chunk " + chunks.get(i).getDensity()
                            + "[CUR] concurrency = " + sp.getConcurrency()
                            + "; parallelism = " + sp.getParallelism()
                            + "; pipelining = " + sp.getPipelining());
                    LOG.info("[SESSION] Chunk " + chunks.get(i).getDensity()
                            + "[CUR] concurrency = " + sp.getConcurrency()
                            + "; parallelism = " + sp.getParallelism()
                            + "; pipelining = " + sp.getPipelining());

                    sp.setConcurrency(tb.getConcurrency());
                    sp.setPipelining(tb.getPipelining());
                    sp.setParallelism(tb.getParallelism());
                    sp.setBufferSize(tb.getBufferSize());
                    chunks.get(i).setTunableParameters(tb);

                } else {
                    SessionParameters sp = new SessionParameters();
                    sp.setConcurrency(tb.getConcurrency());
                    sp.setPipelining(tb.getPipelining());
                    sp.setParallelism(tb.getParallelism());
                    sp.setBufferSize(tb.getBufferSize());
                    sessionParametersMap.put(cType, sp);
                    chunks.get(i).setTunableParameters(tb);
                }

                debugLogger.debug("[DYNAMIC] Chunk: " + chunks.get(i).getDensity() + "; [REQ] Concurrency: " +
                        chunks.get(i).getTunableParameters().getConcurrency() +
                        "; Parallelism: " + chunks.get(i).getTunableParameters().getParallelism() +
                        "; Pipelining: " + chunks.get(i).getTunableParameters().getPipelining() +
                        "; Updated file count: " + chunks.get(i).getRecords().getFileList().size() +
                        "; Chunk Size: " + Utils.printSize(chunks.get(i).getTotalSize(), true));
                LOG.info("[DYNAMIC] Chunk: " + chunks.get(i).getDensity() + "; [REQ] Concurrency: " +
                        chunks.get(i).getTunableParameters().getConcurrency() +
                        "; Parallelism: " + chunks.get(i).getTunableParameters().getParallelism() +
                        "; Pipelining: " + chunks.get(i).getTunableParameters().getPipelining() +
                        "; Updated file count: " + chunks.get(i).getRecords().getFileList().size() +
                        "; Chunk Size: " + Utils.printSize(chunks.get(i).getTotalSize(), true));
                System.out.println("[DYNAMIC] Chunk: " + chunks.get(i).getDensity() + "; [REQ] Concurrency: " + chunks.get(i).getTunableParameters().getConcurrency() +
                        "; Parallelism: " + chunks.get(i).getTunableParameters().getParallelism() +
                        "; Pipelining: " + chunks.get(i).getTunableParameters().getPipelining() +
                        "; Updated file count = " + chunks.get(i).getRecords().getFileList().size() +
                        "; Chunk Size: " + Utils.printSize(chunks.get(i).getTotalSize(), true));
            }
        }
    }

    private void allocateChannelsOnDemand(int maxConcForThisChunk, int desiredConcurrency, FileCluster chunk,
                                          boolean isConcurrencyChange, boolean isPipeChange) throws InterruptedException {
        if (printSysOut)
            System.err.println("----------CHANNEL DISTRIBUTION ON DEMAND STARTED... ");
        LOG.info("----------CHANNEL DISTRIBUTION ON DEMAND STARTED... ");

        String chunkType = chunk.getDensity().toString();
        if (printSysOut)
            System.out.println("Chunk Type = " + chunkType + " IN NEED:"
                    + desiredConcurrency + "; Current channel size:" + chunk.getRecords().channels.size()
                    + "MAX ALLOWED : " + maxConcForThisChunk + " available channel count = " + GridFTPClient.TransferChannel.channelPairList.size());

        LOG.info("Chunk Type = " + chunkType + " IN NEED:"
                + desiredConcurrency + "; Current channel size:" + chunk.getRecords().channels.size()
                + "MAX ALLOWED : " + maxConcForThisChunk + " available channel count = " + GridFTPClient.TransferChannel.channelPairList.size());

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
                if (printSysOut)
                    System.out.println("Channel = " + chunk.getRecords().channels.get(i) + " going to mark as remove..");
                LOG.info("Channel = " + chunk.getRecords().channels.get(i) + " going to mark as remove..");

                synchronized (chunk.getRecords().channels.get(i)) {
                    if (printSysOut)
                        System.out.println("[SYNC] Channel = " + chunk.getRecords().channels.get(i) + " going to mark as remove..");
                    LOG.info("[SYNC] Channel = " + chunk.getRecords().channels.get(i) + " going to mark as remove..");
                    chunk.getRecords().channels.get(i).setMarkedAsRemove(true);
                    list.add(chunk.getRecords().channels.get(i));
                }
            }
            if (printSysOut)
                System.out.println(chunk.getRecords().channels.size() - desiredConcurrency + " channel marked as remove.");
            LOG.info(chunk.getRecords().channels.size() - desiredConcurrency + " channel marked as remove.");
            while (list.size() != 0) {
                Thread.sleep(1); // give a chance to breath
                if (list.get(0).inTransitFiles.size() == 0) {
                    list.remove(0);
                }
            }
            if (printSysOut)
                System.out.println("Channels decreased to size = " + chunk.getRecords().channels.size());
            LOG.info("Channels decreased to size = " + chunk.getRecords().channels.size());

            if (isPipeChange) {
                if (printSysOut)
                    System.out.println("Pipelining change need. Old pipelining: "
                            + chunk.getRecords().channels.get(0).getPipelining()
                            + " new pipelining = " + chunk.getTunableParameters().getPipelining());
                LOG.info("Pipelining change need. Old pipelining: "
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
            if (printSysOut)
                System.out.println("Only pipeline change need. Changing. Old pipelining = "
                        + chunk.getRecords().channels.get(0).getPipelining() +
                        " new pipeline = " + chunk.getTunableParameters().getPipelining());
            LOG.info("Only pipeline change need. Changing. Old pipelining = "
                    + chunk.getRecords().channels.get(0).getPipelining() +
                    " new pipeline = " + chunk.getTunableParameters().getPipelining());
            changePipeline(chunk); // helper
            return;
        }

        // that mean we have some channels in use for this chunk
        // so need to figure out how many new required
        if (chunk.getRecords().channels.size() != 0 && desiredConcurrency > chunk.getRecords().channels.size()) {
            desiredConcurrency = desiredConcurrency - chunk.getRecords().channels.size();
        }
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

        if (desiredConcurrency > availableChannelCount) {
            if (printSysOut)
                System.out.println("desired conc is bigger than available channel count will create new channels after transfer start....");
            LOG.info("desired conc is bigger than available channel count will create new channels after transfer start....");
            isExtraChannelNeeded = true;
            extraChCount = desiredConcurrency - availableChannelCount;
            desiredConcurrency = availableChannelCount;
            if (desiredConcurrency == 0) return;

        }

        // if we come so far, we need to add new channels. so pick channels from available sets.
        // if there is not enough available channels then satisfy as much as we can.
        if (printSysOut)
            System.out.println("Need abs channel count = " + desiredConcurrency);
        LOG.info("Need abs channel count = " + desiredConcurrency);

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

        if (printSysOut)
            System.out.println("Concurrency change done. Pipelining change...");
        LOG.info("Concurrency change done. Pipelining change...");
        changePipeline(chunk);
        if (printSysOut)
            System.out.println("Pipelining change done.");
        LOG.info("Pipelining change done.");

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
            if (c.getPipelining() != chunk.getTunableParameters().getPipelining()) {
                synchronized (c) {
                    c.setPipelining(chunk.getTunableParameters().getPipelining());
                }
            }
        }
    }

    private void channelSettingsOfTransferringChunk(FileCluster chunk, SessionParameters sessionParameters) {
        System.err.println("Requested settings: paral" + sessionParameters.getParallelism() + "; pipe: " + sessionParameters.getPipelining());
        LOG.info("Requested settings: paral" + sessionParameters.getParallelism() + "; pipe: " + sessionParameters.getPipelining());
        System.out.println("CHANNEL SETTINGS BEFORE TRANSFER: ");
        LOG.info("CHANNEL SETTINGS BEFORE TRANSFER: ");
        for (ChannelModule.ChannelPair c : chunk.getRecords().channels) {
            System.out.println("Channel " + c.getId() + "; paral: " + c.parallelism + "; pipe: " + c.getPipelining());
            LOG.info("Channel " + c.getId() + "; paral: " + c.parallelism + "; pipe: " + c.getPipelining());
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

    private boolean parallelismChange(SessionParameters sessionParameters, FileCluster chunk) throws InterruptedException {
        int requestedParallelism = sessionParameters.getParallelism();
        int i;
        boolean needChange = false;

        for (i = chunk.getRecords().channels.size() - 1; i >= 0; i--) {
//            System.out.println("i = " + " channel " + chunk.getRecords().channels.get(i).getId() + " paral = " + chunk.getRecords().channels.get(i).parallelism);
            LOG.info("i = " + " channel " + chunk.getRecords().channels.get(i).getId() + " paral = " + chunk.getRecords().channels.get(i).parallelism);
            if (chunk.getRecords().channels.get(i).parallelism != requestedParallelism) {
                ChannelModule.ChannelPair cp = chunk.getRecords().channels.get(i);
                if (printSysOut)
                    System.out.println("Channel " + cp.getId() + " need restart. Cur parallelism = " + cp.parallelism + " need for = " + requestedParallelism);
                LOG.info("Channel " + cp.getId() + " need restart. Cur parallelism = " + cp.parallelism + " need for = " + requestedParallelism);
                cp.newChunk = chunk;
                cp = restartChannel(cp);
                if (printSysOut)
                    System.out.println("After restart parallelism = " + cp.parallelism);
                LOG.info("After restart parallelism = " + cp.parallelism);
                Runnable runs = new RunTransfers(cp);
                GridFTPClient.executor.submit(runs);
                needChange = true;
            }
        }
        if (needChange) {
            System.out.println("Channels restarted. ");
            channelSettingsOfTransferringChunk(chunk, sessionParameters);
            LOG.info("Channels restarted.");
        } else {
            if (printSysOut)
                System.err.println("NO NEED RESTART CHANNELS");
            LOG.info("NO NEED RESTART CHANNELS");
        }
        return needChange;
    }

    private synchronized ChannelModule.ChannelPair restartChannel(ChannelModule.ChannelPair oldChannel) throws InterruptedException {
        if (printSysOut)
            System.out.println("Updating channel " + oldChannel.getId() + " parallelism to " +
                    oldChannel.newChunk.getTunableParameters().getParallelism());
        LOG.info("Updating channel " + oldChannel.getId() + " parallelism to " +
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
//        System.out.println("*** old channel marked as remove.... ");
        LOG.info("*** old channel marked as remove.... ");
        while (oldChannel.inTransitFiles.size() != 0) {
            Thread.sleep(1);/*waiting for all transfer completion for this channel.*/
        }
//        System.out.println("OLD Channel intransit size = " + oldChannel.inTransitFiles.size());
        LOG.info("OLD Channel intransit size = " + oldChannel.inTransitFiles.size());

        ArrayList<ChannelModule.ChannelPair> list = channelsWithParallelismCountMap.get(oldChannel.parallelism);
        if (list.size() > 0) {
            list.remove(oldChannel);
        }
//        System.out.println("Oldchannel removed from hashmap contains: " + list.contains(oldChannel));
        oldChannel.close();
//        System.out.println("Old channel closed. ");
        newChannel = new ChannelModule.ChannelPair(GridFTPClient.su, GridFTPClient.du);
        boolean success = GridFTPClient.setupChannelConf(newChannel, oldChannel.getId(), oldChannel.newChunk, fileToStart);
//        System.out.println("success:: " + success);
        if (!success) {
            synchronized (newFileList) {
                newFileList.addEntry(fileToStart);
                return null;
            }
        }
        smallMarkedChannels.remove(oldChannel);
        largeMarkedChannels.remove(oldChannel);
        channelInUse.remove(oldChannel);

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
        if (printSysOut)
            System.out.println("restartChannel end  = " + newChannel.parallelism);
        LOG.info("restartChannel end  = " + newChannel.parallelism);
//        System.out.println("New CHANNEL CHUNK: " + newChannel.chunk.getRecords().size());
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

    private XferList.MlsxEntry synchronizedPop(List<XferList.MlsxEntry> fileList) {
        synchronized (fileList) {
            return fileList.remove(0);
        }
    }

}
