package client;

import client.hysterisis.Entry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import transfer_protocol.module.ChannelModule;

import java.io.*;

public class ConfigurationParams {
  private static final Log LOG = LogFactory.getLog(ChannelModule.class);
  public static String INPUT_DIR = "/Users/earslan/HARP/historical_data/activeFiles/";
  public static String OUTPUT_DIR = "/Users/earslan/HARP/target/";
  public static long MAXIMUM_SINGLE_FILE_SIZE = 1024 * 1024 * 1024; // 1GB
  public static double cc_rate = 0.7;
  public static double p_rate = 0.7;
  public static double ppq_rate = 0.99;

  String source, destination, testbed;
  double bandwidth, rtt, bufferSize;
  int maxConcurrency;

  TransferAlgorithm algorithm = TransferAlgorithm.MULTICHUNK;

  boolean channelLevelDebug = false;
  public static boolean isStaticTransfer = false;
  public static boolean parallismOptimization = false;
  int perfFreq = 3;

  String proxyFile;
  ChannelDistributionPolicy channelDistPolicy = ChannelDistributionPolicy.ROUND_ROBIN;
  boolean useHysterisis = false;


  static boolean useOnlineTuning = false;
  boolean useDynamicScheduling = false;

  boolean useMaxCC = false;
  public static int maximumChunks = 2;

  boolean enableIntegrityVerification = false;

  static void init() {
    String home_dir_path = new File("").getAbsolutePath();
    INPUT_DIR = home_dir_path + "/historical_data/activeFiles/";
    OUTPUT_DIR = home_dir_path + "/target";
  }

  void parseArguments (String[] arguments, Entry transferTask) {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    String configFile = "config.cfg";
    InputStream is;
    try {
      if (arguments.length > 0) {
        is = new FileInputStream(arguments[0]);
      } else {
        is = classloader.getResourceAsStream(configFile);
      }

      BufferedReader br = new BufferedReader(new InputStreamReader(is));
      String line;
      while ((line = br.readLine()) != null) {
        processParameter(line.split("\\s+"));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    /*
    Some command line arguments needs values whereas others don't
    Example: "-rtt 32 -use-hysteresis"; where the value after "-rtt" is used to set rtt value whereas
    "-use-hysteresis" just turns hysteresis knob on without needing additional parameters
     */
    for (int i = 1; i < arguments.length; i++) {
      String key = arguments[i];
      String value = "";
      if (i + 1 < arguments.length) {
        value = arguments[i + 1];
      }
      boolean keyValuePair =  processParameter(key, value);
      if (keyValuePair) {
        i++;
      }
    }

    if (proxyFile == null) {
      int uid = findUserId();
      File x509 = new File("/tmp/x509up_u" + uid);
      if (x509.exists())
        this.proxyFile = x509.getAbsolutePath();
    }
    ConfigurationParams.init();
  }

  private boolean processParameter(String... args) {
    String config = args[0];
    boolean usedSecondArgument = true;
    if (config.startsWith("#")) {
      return !usedSecondArgument;
    }
    switch (config) {
      case "-s":
      case "-source":
        if (args.length > 1) {
          source = args[1];
        } else {
          LOG.fatal("-source requires source address");
        }
        LOG.info("source  = " + source);
        break;
      case "-d":
      case "-destination":
        if (args.length > 1) {
          destination = args[1];
        } else {
          LOG.fatal("-destination requires a destination address");
        }
        LOG.info("destination = " + destination);
        break;
      case "-proxy":
        if (args.length > 1) {
          proxyFile = args[1];
        } else {
          LOG.fatal("-spath requires spath of file/directory to be transferred");
        }
        LOG.info("proxyFile = " + proxyFile);
        break;
      case "-bw":
      case "-bandwidth":
        if (args.length > 1 || Double.parseDouble(args[1]) > 100) {
         bandwidth = Math.pow(10, 9) * Double.parseDouble(args[1]);
        } else {
          LOG.fatal("-bw requires bandwidth in GB");
        }
        LOG.info("bandwidth = " + bandwidth + " GB");
        break;
      case "-rtt":
        if (args.length > 1) {
          rtt = Double.parseDouble(args[1]);
        } else {
          LOG.fatal("-rtt requires round trip time in millisecond");
        }
        LOG.info("rtt = " + rtt + " ms");
        break;
      case "-maxcc":
      case "-max-concurrency":
        if (args.length > 1) {
          maxConcurrency = Integer.parseInt(args[1]);
        } else {
          LOG.fatal("-cc needs integer");
        }
        LOG.info("cc = " + maxConcurrency);
        break;
      case "-bs":
      case "-buffer-size":
        if (args.length > 1) {
          bufferSize = Double.parseDouble(args[1]) * 1024 * 1024; //in MB
        } else {
          LOG.fatal("-bs needs integer");
        }
        LOG.info("bs = " + bufferSize);
        break;
      case "-testbed":
        if (args.length > 1) {
          testbed = args[1];
        } else {
          LOG.fatal("-testbed needs testbed name");
        }
        LOG.info("Testbed name is = " + testbed);
        break;
      case "-input":
        if (args.length > 1) {
          ConfigurationParams.INPUT_DIR = args[1];
        } else {
          LOG.fatal("-historical data input file spath has to be passed");
        }
        LOG.info("Historical data spath = " + ConfigurationParams.INPUT_DIR);
        break;
      case "-maximumChunks":
        if (args.length > 1) {
          maximumChunks = Integer.parseInt(args[1]);
        } else {
          LOG.fatal("-maximumChunks requires an integer value");
        }
        LOG.info("Number of fileClusters = " + maximumChunks);
        break;
      case "-use-hysteresis":
        useHysterisis = true;
        LOG.info("Use hysteresis based approach");
        break;
      case "-single-chunk":
        algorithm = TransferAlgorithm.SINGLECHUNK;
        usedSecondArgument = false;
        LOG.info("Use single chunk transfer approach");
        break;
      case "-channel-distribution-policy":
        if (args.length > 1) {
          if (args[1].compareTo("roundrobin") == 0) {
            channelDistPolicy = ChannelDistributionPolicy.ROUND_ROBIN;
          } else if (args[1].compareTo("weighted") == 0) {
            channelDistPolicy = ChannelDistributionPolicy.WEIGHTED;
          } else {
            LOG.fatal("-channel-distribution-policy can be either \"roundrobin\" or \"weighted\"");
          }
        } else {
          LOG.fatal("-channel-distribution-policy has to be specified as \"roundrobin\" or \"weighted\"");
        }
        break;
      case "-use-dynamic-scheduling":
        algorithm = TransferAlgorithm.PROACTIVEMULTICHUNK;
        useDynamicScheduling = true;
        channelDistPolicy = ChannelDistributionPolicy.WEIGHTED;
        usedSecondArgument = false;
        LOG.info("Dynamic scheduling enabled.");
        break;
      case "-use-online-tuning":
        useOnlineTuning = true;
        usedSecondArgument = false;
        LOG.info("Online modelling/tuning enabled.");
        break;
      case "-use-checksum":
        enableIntegrityVerification = true;
        usedSecondArgument = false;
        LOG.info("Checksum enabled.");
        break;
      case "-use-max-cc":
        useMaxCC = true;
        usedSecondArgument = false;
        LOG.info("Use of maximum concurrency enabled.");
        break;
      case "-perf-freq":
        perfFreq = Integer.parseInt(args[1]);
        break;
      case "-enable-channel-debug":
        channelLevelDebug = true;
        usedSecondArgument = false;
        break;
      case "-static":
        isStaticTransfer = true;
        break;
      case "-parOpt":
        parallismOptimization = true;
        break;
      default:
        System.err.println("Unrecognized input parameter " + config);
        System.exit(-1);
    }
    return usedSecondArgument;
  }


  private int findUserId() {
    String userName = null;
    int uid = -1;
    try {
      userName = System.getProperty("user.name");
      String command = "id -u " + userName;
      Process child = Runtime.getRuntime().exec(command);
      BufferedReader stdInput = new BufferedReader(new InputStreamReader(child.getInputStream()));
      String s;
      while ((s = stdInput.readLine()) != null) {
        uid = Integer.parseInt(s);
      }
      stdInput.close();
    } catch (IOException e) {
      System.err.print("Proxy file for user " + userName + " not found!" + e.getMessage());
      System.exit(-1);
    }
    return uid;
  }

  public enum TransferAlgorithm {SINGLECHUNK, MULTICHUNK, PROACTIVEMULTICHUNK}
  public enum ChannelDistributionPolicy {ROUND_ROBIN, WEIGHTED}

}
