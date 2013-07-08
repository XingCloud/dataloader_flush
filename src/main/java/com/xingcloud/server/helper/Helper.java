package com.xingcloud.server.helper;

import com.xingcloud.server.exception.ConfException;
import com.xingcloud.server.tailler.TimeUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * User: IvyTang
 * Date: 12-11-28
 * Time: 下午2:14
 */
public class Helper {

  private static final Log LOG = LogFactory.getLog(Helper.class);

  private static final int ONE_DAY_SECONDS = 24 * 3600;

  public static final long FUTURE_TS_REFERENCE = 4102416000000l;//2100的时间戳

  public static String getDate(long timestamp) {
    SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
    df.setTimeZone(TimeZone.getTimeZone(Constants.TIMEZONE));
    Date date = new Date(timestamp);
    return df.format(date);
  }

  public static long getCurrentDayBeginTimestamp() {
    long currentTsSecond = System.currentTimeMillis() / 1000;
    return (currentTsSecond - currentTsSecond % ONE_DAY_SECONDS) * 1000;
  }

  /**
   * 用2100年时间戳 - 本来的时间戳，作为once的时间戳，让第一次出现的ts变为最大的一个，实现once的目的
   *
   * @return
   */
  public static long transformOnceTimestamp() {

    return FUTURE_TS_REFERENCE - System.currentTimeMillis();
  }


  public static void hbaseFlushCheckpoint(long day, long flushLines) {
    FileWriter checkPointWriter = null;
    try {
      checkPointWriter = new FileWriter(Constants.EVENT_TAIL_CONF_PATH + File.separator + Constants
              .HBASE_FLUSH_POINT);
      checkPointWriter.write(String.valueOf(TimeUtil.getDate(System.currentTimeMillis())));
      checkPointWriter.write("\n");
      checkPointWriter.write(String.valueOf(day));
      checkPointWriter.write("\n");
      checkPointWriter.write(String.valueOf(flushLines));
      checkPointWriter.write("\n");
    } catch (IOException e) {
      LOG.error(e.getMessage());
    } finally {
      try {
        if (checkPointWriter != null)
          checkPointWriter.close();
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }
    }


  }

  public static void restoreConfFromHBaseCrash() throws ConfException {
    BufferedReader hbaseFlushReader = null;
    FileWriter sendProcessWriter = null;
    String sendProcessDay = getDate(System.currentTimeMillis());
    String sendLines = "0";
    try {
      hbaseFlushReader = new BufferedReader(new FileReader(Constants.EVENT_TAIL_CONF_PATH + File.separator
              + Constants.HBASE_FLUSH_POINT));
      List<String> hbaseFlusConfs = new ArrayList<String>();
      String tmpLine = null;
      while ((tmpLine = hbaseFlushReader.readLine()) != null) {
        hbaseFlusConfs.add(tmpLine);
      }
      if (hbaseFlusConfs.size() != 3)
        throw new ConfException("hbase flush conf error.");
      sendProcessDay = hbaseFlusConfs.get(1);
      sendLines = hbaseFlusConfs.get(2);
    } catch (FileNotFoundException e) {
      LOG.info("hbase not flush....set send process to today:0lindes.");
    } catch (IOException e) {
      throw new ConfException(e.getMessage());
    } finally {
      try {
        if (hbaseFlushReader != null)
          hbaseFlushReader.close();
      } catch (IOException e) {
        throw new ConfException(e.getMessage());
      }
    }

    try {
      sendProcessWriter = new FileWriter(Constants.EVENT_TAIL_CONF_PATH + File.separator + Constants
              .SEND_PROCESS);
      sendProcessWriter.write(sendProcessDay + "\t" + sendLines + "\t" + new Date() + "\n");
      sendProcessWriter.flush();
    } catch (IOException e) {
      throw new ConfException(e.getMessage());
    } finally {
      try {
        if (sendProcessWriter != null)
          sendProcessWriter.close();
      } catch (IOException e) {
        throw new ConfException(e.getMessage());
      }
    }

  }


  public static long[] getSendLogPosition() {
    long[] infos = new long[2];
    File process = new File(Constants.EVENT_TAIL_CONF_PATH + File.separator
            + "sendlog.process");
    if (process.exists()) {
      try {
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(process)), 1024 * 1024);
        String rdline = null;
        String tmpline = null;
        while ((tmpline = reader.readLine()) != null) {
          rdline = tmpline;
        }

        if (rdline == null) {
          infos[0] = TimeUtil.getDay(System.currentTimeMillis());
          infos[1] = 0l;
        } else {
          int idxtab_f = 0;
          int idxtab_t = rdline.indexOf('\t');
          infos[0] = Long.valueOf(rdline.substring(idxtab_f, idxtab_t));
          idxtab_f = idxtab_t + 1;
          idxtab_t = rdline.indexOf('\t', idxtab_f);
          infos[1] = Long.valueOf(rdline.substring(idxtab_f, idxtab_t));
        }

      } catch (Exception e) {
        LOG.error(e.getMessage());
      }
    } else {
      infos[0] = TimeUtil.getDay(System.currentTimeMillis());
      infos[1] = 0l;
    }
    return infos;

  }

  public static Set<String> getAllProjects() {
    long currentTime = System.currentTimeMillis();
    Jedis jedis = null;
    Set<String> projects = new HashSet<String>();
    try {
      jedis = RedisResourceManager.getInstance().getCache(Constants.REDIS_UICHECK_NUM);
      Set<String> raw_projects = jedis.keys("ui.check.*");
      for (String raw_project : raw_projects)
        projects.add(raw_project.substring(9));
    } catch (Exception e) {
      RedisResourceManager.getInstance().returnBrokenResource(jedis);
      jedis = null;
    } finally {
      RedisResourceManager.getInstance().returnResource(jedis);
    }
    LOG.info("get projects from redis using " + (System.currentTimeMillis() - currentTime) + "ms.");
    return projects;
  }


  public static boolean checkNeedProcessDelayLog() {
    String localip = getLocalIp();
    Jedis jedis = null;
    try {
      jedis = RedisResourceManager.getInstance().getCache(Constants.REDIS_UICHECK_NUM);
      String needProcess = jedis.get("delaylog_" + localip);
      if (needProcess != null && needProcess.equals("ok"))
        return true;
    } catch (Exception e) {
      RedisResourceManager.getInstance().returnBrokenResource(jedis);
      jedis = null;
    } finally {
      RedisResourceManager.getInstance().returnResource(jedis);
    }
    return false;

  }

  public static String getHBaseTableName(String project) {
    return project + "_deu";
  }

  public static String getLocalIp() {
    String localIp = null;
    Enumeration<NetworkInterface> netInterfaces = null;
    try {
      netInterfaces = NetworkInterface.getNetworkInterfaces();
      InetAddress ip = null;
      while (netInterfaces.hasMoreElements()) {
        NetworkInterface ni = netInterfaces.nextElement();
        Enumeration<InetAddress> address = ni.getInetAddresses();
        while (address.hasMoreElements()) {
          ip = address.nextElement();
          if (ip.isSiteLocalAddress() && !ip.isLoopbackAddress() && !ip.getHostAddress().contains(":")) {
            localIp = ip.getHostAddress();
          }
        }
      }
    } catch (SocketException e) {
      LOG.error(e.getMessage());
    }
    return localIp;

  }


  private static void exch(String[] list, int i, int j) {
    String swap = list[i];
    list[i] = list[j];
    list[j] = swap;
  }

  // take as input an array of strings and rearrange them in random order
  public static void shuffle(String[] list) {
    int N = list.length;
    Random random = new Random();
    for (int i = 0; i < N; i++) {
      int r = random.nextInt(N); // between i and N-1
      System.out.println(r);
      exch(list, i, r);
    }

  }


  public static void main(String[] args) throws InterruptedException {
    long t1 = getCurrentDayBeginTimestamp();
    System.out.println(t1);
    Thread.sleep(3 * 1000);
    long t2 = getCurrentDayBeginTimestamp();
    System.out.println(t2);

    System.out.println(getDate(t1));
    System.out.println(getDate(t2));
  }
}
