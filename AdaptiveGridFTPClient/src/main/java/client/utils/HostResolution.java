package client.utils;

import java.net.InetAddress;

public class HostResolution extends Thread {
    String hostname;
    InetAddress[] allIPs;

    public HostResolution(String hostname) {
        this.hostname = hostname;
    }

    @Override
    public void run() {
        try {
            System.out.println("Getting IPs for:" + hostname);
            allIPs = InetAddress.getAllByName(hostname);
            System.out.println("Found IPs for:" + hostname);
            //System.out.println("IPs for:" + du.getHost());
            for (int i = 0; i < allIPs.length; i++) {
                System.out.println(allIPs[i]);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public InetAddress[] getAllIPs() {
        return allIPs;
    }
}