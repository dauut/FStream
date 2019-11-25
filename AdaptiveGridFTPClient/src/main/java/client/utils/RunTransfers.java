package client.utils;

import client.FileCluster;
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
            System.out.println("CHANNEL ID : " + cc.getId() + "STARTED." + " Chunk = " + cc.chunk.getDensity().toString());
            try{
                GridFTPClient.ftpClient.transferList(cc);
            }catch(Exception e){
                e.printStackTrace();
                e.printStackTrace();
                System.err.println("Exception occured create new channel for this chunk");
                FileCluster chunk = cc.chunk;
                GridFTPClient.hotFixNewChannelExceptioncase(chunk);
                cc.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("RUNTRANSFER THREAD ERROR.");
        }
    }
}
