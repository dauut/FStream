package client.utils;

import transfer_protocol.module.ChannelModule;
import transfer_protocol.module.GridFTPClient;

public class RunTransfers implements Runnable {
    private ChannelModule.ChannelPair cc;

    public RunTransfers(ChannelModule.ChannelPair cc){
        this.cc = cc;
    }
    @Override
    public void run() {
        try {
            GridFTPClient.ftpClient.transferList(cc);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("RUNTRANSFER THREAD ERROR.");
        }
    }
}
