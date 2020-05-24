package client.transfer;

import client.AdaptiveGridFTPClient;
import client.FileCluster;
import client.utils.RunTransfers;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import transfer_protocol.module.ChannelModule;
import transfer_protocol.module.GridFTPClient;
import transfer_protocol.util.XferList;

import java.util.ArrayList;

public class ChannelOperations {

    private static final Log LOG = LogFactory.getLog(ChannelOperations.class);

    private synchronized ChannelModule.ChannelPair restartChannel(ChannelModule.ChannelPair oldChannel) throws InterruptedException {
        if (AdaptiveGridFTPClient.printSysOut)
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
        oldChannel.setMarkedAsRemove(true);
        LOG.info("*** old channel marked as remove.... ");
        while (oldChannel.inTransitFiles.size() != 0) {
            Thread.sleep(1);/*waiting for all transfer completion for this channel.*/
        }
        LOG.info("OLD Channel intransit size = " + oldChannel.inTransitFiles.size());

        ArrayList<ChannelModule.ChannelPair> list = AdaptiveGridFTPClient.channelsWithParallelismCountMap.get(oldChannel.parallelism);
        if (list.size() > 0) {
            list.remove(oldChannel);
        }
        oldChannel.close();
        newChannel = new ChannelModule.ChannelPair(GridFTPClient.su, GridFTPClient.du);
        boolean success = GridFTPClient.setupChannelConf(newChannel, oldChannel.getId(), oldChannel.newChunk, fileToStart);
        if (!success) {
            synchronized (newFileList) {
                newFileList.addEntry(fileToStart);
                return null;
            }
        }
        AdaptiveGridFTPClient.smallMarkedChannels.remove(oldChannel);
        AdaptiveGridFTPClient.largeMarkedChannels.remove(oldChannel);
        AdaptiveGridFTPClient.channelInUse.remove(oldChannel);
        synchronized (newFileList.channels) {
            newFileList.channels.add(newChannel);
        }
        updateOnAir(newFileList, +1);
        if (AdaptiveGridFTPClient.printSysOut)
            System.out.println("restartChannel end  = " + newChannel.parallelism);
        LOG.info("restartChannel end  = " + newChannel.parallelism);
        return newChannel;
    }

    public void parallelismChange(ChannelModule.ChannelPair cp, FileCluster chunk) throws InterruptedException {

        cp.newChunk = chunk;
        cp = restartChannel(cp);
        if (AdaptiveGridFTPClient.printSysOut)
            System.out.println("After restart parallelism = " + cp.parallelism);
        LOG.info("After restart parallelism = " + cp.parallelism);
        Runnable runs = new RunTransfers(cp);
        GridFTPClient.executor.submit(runs);


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


}
