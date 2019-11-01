package client.utils;

import client.AdaptiveGridFTPClient;
import client.FileCluster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import transfer_protocol.module.GridFTPClient;

public class MonitorTransfer extends Thread{
    AdaptiveGridFTPClient main;
    private static final Log LOG = LogFactory.getLog(MonitorTransfer.class);

    public MonitorTransfer(AdaptiveGridFTPClient main){
        this.main = main;
    }

    public void run(){
        System.err.println("Monitor transfer runs.");
        long start = System.currentTimeMillis();
        long timespent = 0;
        for (FileCluster fileCluster : GridFTPClient.ftpClient.fileClusters){
            try {
                while (fileCluster.getRecords().totalTransferredSize < fileCluster.getRecords().initialSize) {
                    LOG.info(fileCluster.getRecords().totalTransferredSize + " " + fileCluster.getRecords().initialSize);
                    Thread.sleep(400);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
        timespent+=(System.currentTimeMillis() - start) / 1000;
        System.err.println("Transfer:" + AdaptiveGridFTPClient.TRANSFER_NUMBER + " completed in: " + timespent);
        AdaptiveGridFTPClient.isTransfersCopmletedMap.put(AdaptiveGridFTPClient.TRANSFER_NUMBER, true);
        System.out.println("Remove transfer information..");
        main.cleanCurrentTransferInformation();
        System.out.println("Cleaner done!");
        AdaptiveGridFTPClient.TRANSFER_NUMBER++;
    }
}
