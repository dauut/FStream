package client.transfer;

import client.AdaptiveGridFTPClient;
import client.ConfigurationParams;
import client.FileCluster;
import client.utils.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import transfer_protocol.module.ChannelModule;
import transfer_protocol.util.SessionParameters;

public class ProfilingOps {

    private static final Log LOG = LogFactory.getLog(ProfilingOps.class);
//    private static ChannelOperations channelOperations;
    private static ChannelOperations channelOperations = new ChannelOperations();

    public static void chunkProfiling(int maxConcurrency, ConfigurationParams.ChannelDistributionPolicy channelDistPolicy) {
        LOG.info("-------------PROFILING START------------------");
        System.out.println("-------------PROFILING START------------------");

        double upperLimitInit = ConfigurationParams.upperLimitInit;
        double upperLimit = ConfigurationParams.upperLimit;
        double totalThroughput = getAverageThroughput();
        double low = upperLimitInit; //- upperLimitInit * 20 / 100;

        if (totalThroughput > low) {
            System.err.println("CONT. NO NEED PROFILING");
            AdaptiveGridFTPClient.counterOfProfilingChanger++;
        } else {
            double perChannelThroughput;
            perChannelThroughput = totalThroughput / AdaptiveGridFTPClient.channelInUse.size();
            LOG.info("upperLimit = " + upperLimit + " perChannelThroughput = " + perChannelThroughput);
            if (AdaptiveGridFTPClient.printSysOut)
                System.out.println("upperLimit = " + upperLimit + " perChannelThroughput = " + perChannelThroughput);

            upperLimit = upperLimitInit; //* ConfigurationParams.percentageRate / 80;
            if (AdaptiveGridFTPClient.printSysOut)
                System.err.println("upperLimit: " + upperLimit + " || upperLimitInit: " + upperLimitInit);

            int possibleConcCount = (int) ((int) upperLimit / perChannelThroughput);
            if (AdaptiveGridFTPClient.printSysOut)
                System.out.println("POssible ch count = " + possibleConcCount);
            if (possibleConcCount > AdaptiveGridFTPClient.channelInUse.size()) {

                if (possibleConcCount > AdaptiveGridFTPClient.channelInUse.size() * 2) {
                    possibleConcCount = AdaptiveGridFTPClient.channelInUse.size() * 2;
                }

                if (possibleConcCount > maxConcurrency) {
                    possibleConcCount = maxConcurrency;
                }

                if (AdaptiveGridFTPClient.printSysOut)
                    System.out.println("NEW POSSIBLE CHANNEL COUNT ===== " + possibleConcCount);
                Utils.allocateChannelsToChunks(AdaptiveGridFTPClient.chunks, possibleConcCount,channelDistPolicy);
                if (AdaptiveGridFTPClient.printSysOut)
                    System.out.println("Allocation copmleted: ");
                setProfilingSettings();
            }
        }
        LOG.info("-------------PROFILING END------------------");
        System.out.println("-------------PROFILING END------------------");
    }

    public static void qosProfiling(int maxConcurrency, ConfigurationParams.ChannelDistributionPolicy channelDistPolicy) {
        LOG.info("-------------QoS START------------------");
        System.out.println("-------------QoS START------------------");

        double upperLimit = ConfigurationParams.speedLimit;

        int curChannelCount = AdaptiveGridFTPClient.channelInUse.size();
        double totalThroughput = getAverageThroughput();
        double totalThroughput1 = AdaptiveGridFTPClient.avgThroughput.get(AdaptiveGridFTPClient.avgThroughput.size() - 1);

        double perChannelThroughput;
        perChannelThroughput = totalThroughput / AdaptiveGridFTPClient.channelInUse.size();
        System.out.println("PER CHANNEL THROUGHPUT = " + perChannelThroughput);

        int diff = Math.abs((int) (totalThroughput - upperLimit));
        int diff1 = Math.abs((int) (totalThroughput1 - upperLimit));
        int newChannelCount = curChannelCount;
        System.out.println("CURRENT CHANNEL COUNT = " + curChannelCount);

        if (totalThroughput > upperLimit) {
            if ((diff > ((int) upperLimit * 15 / 100)) && (diff1 > ((int) upperLimit * 15 / 100))) {
                newChannelCount = (int) (upperLimit / perChannelThroughput);
                AdaptiveGridFTPClient.counterOfProfilingChanger = 0;
                System.out.println("QoS profiled. New channel count = " + newChannelCount);
            }
            if (AdaptiveGridFTPClient.counterOfProfilingChanger > 3 || (diff > ((int) upperLimit * 5 / 100) && diff < ((int) upperLimit * 15 / 100))) {
                System.err.println("The speed was reliable, fine tuning is started.....");
                qOSChangePipeAndPar(-1);
                AdaptiveGridFTPClient.counterOfProfilingChanger = 0;
            } else {
                System.err.println("CONT. NO NEED PROFILING");
                AdaptiveGridFTPClient.counterOfProfilingChanger++;
            }
        } else {
            if ((diff > ((int) upperLimit * 20 / 100)) && (diff1 > ((int) upperLimit * 20 / 100))) {
                newChannelCount = (int) (upperLimit / perChannelThroughput);
                System.out.println("QoS profiled. New channel count = " + newChannelCount);
                AdaptiveGridFTPClient.counterOfProfilingChanger = 0;
            }
            //avoid dramatic changes
            if (newChannelCount > curChannelCount * 2) {
                newChannelCount = curChannelCount * 2;
            }
            if (newChannelCount > maxConcurrency) {
                newChannelCount = maxConcurrency;
            }

            if (AdaptiveGridFTPClient.counterOfProfilingChanger > 2 || (diff > ((int) upperLimit * 5 / 100) && diff < ((int) upperLimit * 10 / 100))) {
                qOSChangePipeAndPar(1);
                AdaptiveGridFTPClient.counterOfProfilingChanger = 0;
            }

        }
        if (newChannelCount != curChannelCount) {
            System.out.println("NEW CHANNEL COUNT = " + newChannelCount);
            AdaptiveGridFTPClient.debugLogger.info("NEW CHANNEL COUNT = " + newChannelCount);
            Utils.allocateChannelsToChunks(AdaptiveGridFTPClient.chunks, newChannelCount, channelDistPolicy);
            setProfilingSettings();
        }
        LOG.info("-------------QoS ENDED------------------");
        System.out.println("-------------QoS ENDED------------------");
    }


    private static void setProfilingSettings() {
        for (FileCluster f : AdaptiveGridFTPClient.chunks) {
            if (AdaptiveGridFTPClient.printSysOut)
                System.out.println("HISTORY ------------ Chunk = " + f.getDensity().toString() + " new channel count = " + f.getTunableParameters().getConcurrency());
            AdaptiveGridFTPClient.historicalProfiling.put(f.getDensity().toString(), f.getTunableParameters().getConcurrency());
            if (AdaptiveGridFTPClient.printSysOut)
                System.out.println(f.getDensity() + " conc = " + f.getTunableParameters().getConcurrency());
            SessionParameters sp = AdaptiveGridFTPClient.sessionParametersMap.get(f.getDensity().toString());
            sp.setConcurrency(f.getTunableParameters().getConcurrency());
            AdaptiveGridFTPClient.sessionParametersMap.put(f.getDensity().toString(), sp);
        }
    }

    private static double getAverageThroughput() {
        double tp = 0;
        for (int i = AdaptiveGridFTPClient.avgThroughput.size() - 1; i > AdaptiveGridFTPClient.avgThroughput.size() - 4; i--) {
            double tmp = AdaptiveGridFTPClient.avgThroughput.get(i);
            tp += tmp;
        }
        return tp / 3;
    }

    private static void qOSChangePipeAndPar(int isUp) {
        for (FileCluster f : AdaptiveGridFTPClient.chunks) {
            boolean parChanged = false;
            for (ChannelModule.ChannelPair c : f.getRecords().channels) {
                if (f.getDensity().toString().equals("SMALL") && c.getPipelining() >= 1) {
                    System.out.println("Channel: " + c.getId() + " PIPELINING changed from > " + c.getPipelining() + " to " + (c.getPipelining() - (isUp)));
                    AdaptiveGridFTPClient.debugLogger.info("Channel: " + c.getId() + " PIPELINING changed from > " + c.getPipelining() + " to " + (c.getPipelining() - (isUp)));
                    f.getTunableParameters().setPipelining(c.getPipelining() - (isUp));
                    c.setPipelining(c.getPipelining() - (isUp));
                }

                if (f.getDensity().toString().equals("LARGE") && c.parallelism >= 1 && !parChanged) {
                    int par = c.parallelism - (isUp);
                    System.out.println("Channel: " + c.getId() + "parallelism changed from" + c.parallelism + "to > " + (c.parallelism - (isUp)));
                    AdaptiveGridFTPClient.debugLogger.info(("Channel: " + c.getId() + "parallelism changed from" + c.parallelism + "to > " + (c.parallelism - (isUp))));
                    f.getTunableParameters().setParallelism(par);
                    System.err.println("RESET CHANNEL > > > > " + f.getTunableParameters().getParallelism());
                    AdaptiveGridFTPClient.debugLogger.info(("RESET CHANNEL > > > > " + f.getTunableParameters().getParallelism()));
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        channelOperations.parallelismChange(c, f);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    parChanged = true;
                }

            }
        }
    }

}
