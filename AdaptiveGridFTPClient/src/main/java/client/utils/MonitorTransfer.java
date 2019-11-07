package client.utils;

import client.AdaptiveGridFTPClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MonitorTransfer extends Thread {
    private AdaptiveGridFTPClient main;
    private static final Log LOG = LogFactory.getLog(MonitorTransfer.class);
    private final static Logger debugLogger = LogManager.getLogger("reportsLogger");

    public MonitorTransfer(AdaptiveGridFTPClient main) {
        this.main = main;
    }

    public void run() {
        System.err.println("Monitor transfer runs.");
        long start = System.currentTimeMillis();
        long timeSpent = 0;
        while (AdaptiveGridFTPClient.channelInUse.size() != 0) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Monitored datasize = " + AdaptiveGridFTPClient.dataSizeofCurrentTransfer);
        timeSpent += (System.currentTimeMillis() - start) / 1000;
        System.err.println("Transfer:" + AdaptiveGridFTPClient.TRANSFER_NUMBER + " completed in: " + timeSpent
                + " throughput: " + (AdaptiveGridFTPClient.dataSizeofCurrentTransfer * 8.0)
                / (timeSpent * (1000.0 * 1000)));
        debugLogger.debug(timeSpent + "\t" + (AdaptiveGridFTPClient.dataSizeofCurrentTransfer * 8.0)
                / (timeSpent * (1000.0 * 1000)));
        LOG.info("Throughput : " + (AdaptiveGridFTPClient.dataSizeofCurrentTransfer * 8.0)
                / (timeSpent * (1000.0 * 1000)) + " Time: " + timeSpent);
        AdaptiveGridFTPClient.isTransfersCopmletedMap.put(AdaptiveGridFTPClient.TRANSFER_NUMBER, true);
        System.out.println("Remove transfer information..");
        System.out.println("Cleaner done!");
        AdaptiveGridFTPClient.TRANSFER_NUMBER++;
        main.cleanCurrentTransferInformation();
    }
}
