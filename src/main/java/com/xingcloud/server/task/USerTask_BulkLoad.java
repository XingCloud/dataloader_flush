package com.xingcloud.server.task;


import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.mysql.UpdateFunc;
import com.xingcloud.server.helper.Constants;
import com.xingcloud.server.helper.Helper;
import com.xingcloud.server.helper.ProjectPropertyCache;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;

/**
 * User: IvyTang
 * Date: 13-5-13
 * Time: 上午9:54
 */
public class USerTask_BulkLoad implements Runnable {

  private static final Log LOG = LogFactory.getLog(USerTask_BulkLoad.class);

  private String project;
  private List<User_BulkLoad> users;
  private ProjectPropertyCache projectPropertyCache;
  private Map<String, Connection> cons = null;
  private int dead_lock_sleep_time = 5 * 1000;


  public USerTask_BulkLoad(String project, List<User_BulkLoad> users) {
    this.project = project;
    this.users = users;
    this.projectPropertyCache = ProjectPropertyCache.resetProjectCache(project);
    this.cons = new HashMap<String, Connection>();
  }


  @Override
  public void run() {

    try {
      LOG.info("enter user task:user size:" + users.size() + "\t" + project);

      Map<String, BufferedWriter> writers = new HashMap<String, BufferedWriter>();
      Map<String, Set<String>> nodeTables = new HashMap<String, Set<String>>();
      Map<String, List<String>> incSqls = new HashMap<String, List<String>>();

      long currentTime = System.currentTimeMillis();
      prepareUsers(writers, incSqls, nodeTables);
      LOG.info("USerTask_BulkLoad==== " + project + " prepareUsers using time: " + (System.currentTimeMillis() - currentTime)
              + " ms.size:" + users.size());

      currentTime = System.currentTimeMillis();
      incSqlsLoadToMySQL(incSqls);
      LOG.info("USerTask_BulkLoad==== " + project + " incSqlsLoadToMySQL using time: " + (System.currentTimeMillis() - currentTime)
              + " ms.size:" + users.size());

      currentTime = System.currentTimeMillis();
      bulkLoad(nodeTables);
      LOG.info("USerTask_BulkLoad==== " + project + " bulkLoad using time: " + (System.currentTimeMillis() - currentTime)
              + " ms.size:" + users.size());
    } catch (Exception e) {
      LOG.error("end this thread .", e);
    }

  }


  /**
   * 分析user.log，生成每个node对应的load data文件
   *
   * @param writers
   * @param incSqls
   * @param nodeTables
   */
  private void prepareUsers(Map<String, BufferedWriter> writers, Map<String, List<String>> incSqls, Map<String,
          Set<String>> nodeTables) {
    // do the prepare work
    try {
      //sort  List<User_BulkLoad> users ,let them sort in user hashuid sort.
      Collections.sort(users, new UserComparator());

      for (User_BulkLoad user : users) {
        List<String> propKeys = user.getPropKeys();
        List<String> propValues = user.getPropValues();
        String nodeAddress = UidMappingUtil.getInstance().hash(user.getSeqUid());
        Set<String> nProps = nodeTables.get(nodeAddress);
        if (nProps == null) {
          nProps = new TreeSet<String>();
          nodeTables.put(nodeAddress, nProps);
        }
        nProps.addAll(propKeys);

        //check in local cache
        //boolean inLocalHotCache = HotUserProjectBitmap.getInstance().ifInLocalCacheHot(this.project, user.getSeqUid());
        //if (!inLocalHotCache) {
        //  if (!checkInHot(user))
        //    cold2Hot(user);
        //  HotUserProjectBitmap.getInstance().markLocalCacheHot(this.project, user.getSeqUid());
        // }

        for (int i = 0; i < user.getPropKeys().size(); i++) {
          String key = propKeys.get(i);
          String value = propValues.get(i);

          if (projectPropertyCache.getUserPro(key).getPropFunc() == UpdateFunc.inc) {
            String incSql = "INSERT INTO `" + key + "` (uid,val) VALUES (" + user.getSamplingUid() + "," +
                    value + ") ON DUPLICATE KEY UPDATE val=val+" + value + ";";

            List<String> sqls = incSqls.get(nodeAddress);
            if (sqls == null) {
              sqls = new ArrayList<String>();
              incSqls.put(nodeAddress, sqls);
            }
            sqls.add(incSql);
          } else {
            BufferedWriter bufferedWriter = writers.get(nodeAddress + "_" + key);
            if (bufferedWriter == null) {
              bufferedWriter = new BufferedWriter(new FileWriter(Constants.USER_LOAD_PATH
                      + project + "_" + nodeAddress + "_" + key));
              writers.put(nodeAddress + "_" + key, bufferedWriter);
            }
            bufferedWriter.write(user.getSamplingUid() + "\t" + value);
            bufferedWriter.newLine();
          }
        }
      }
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    //catch (SQLException e) {
    //  LOG.error(e.getMessage());
    // }
    finally {
      for (Map.Entry<String, BufferedWriter> entry : writers.entrySet()) {
        try {
          entry.getValue().flush();
          entry.getValue().close();
        } catch (IOException e) {
          LOG.error(e.getMessage());
        }
      }
    }
  }

  /**
   * inc的属性不能通过load data，还是通过mysql connection statement。
   *
   * @param incSqls
   */
  private void incSqlsLoadToMySQL(Map<String, List<String>> incSqls) throws InterruptedException {
    //sqls
    try {
      for (Map.Entry<String, List<String>> entry : incSqls.entrySet()) {
        Collections.sort(entry.getValue());
        Connection connection = getNodeConn(project, entry.getKey());
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();
        int loopTimes = entry.getValue().size() / Constants.MYSQL_BATCH_UPDATE_SIZE + (entry.getValue().size() %
                Constants.MYSQL_BATCH_UPDATE_SIZE > 0 ? 1 : 0);
        for (int i = 0; i < loopTimes; i++) {
          statement.clearBatch();
          for (int j = 0; j < Constants.MYSQL_BATCH_UPDATE_SIZE; j++) {
            int index = i * Constants.MYSQL_BATCH_UPDATE_SIZE + j;
            if (index == entry.getValue().size())
              break;
            statement.addBatch(entry.getValue().get(index));
          }
          statement.executeBatch();
          connection.commit();
        }
        statement.close();
        connection.setAutoCommit(true);
      }
    } catch (SQLException e) {
      while (true) {
        LOG.error("incSqlsLoadToMySQL error." + e.getMessage());
        Thread.sleep(dead_lock_sleep_time);
      }
    } finally {
      for (Map.Entry<String, Connection> entry : cons.entrySet())
        try {
          entry.getValue().close();
        } catch (SQLException e) {
          LOG.error(e.getMessage());
        }
    }
  }


  /**
   * bulk load，机器和每个机器对应的表 顺序 乱序，尽量减少出现2个进程同时load data一个表，会发生deadlock（数据不会丢，程序会重试）。
   *
   * @param nodeTables
   */
  private void bulkLoad(Map<String, Set<String>> nodeTables) throws InterruptedException {
    //bulk load

    List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();

    for (Map.Entry<String, Set<String>> entry : nodeTables.entrySet()) {
      for (String tableName : entry.getValue()) {
        UpdateFunc updateFunc = projectPropertyCache.getUserPro(tableName).getPropFunc();
        if (updateFunc == null) {
          LOG.info(tableName + " proper is null.IMPORT====");
          continue;
        }
        String filePath = Constants.USER_LOAD_PATH + project + "_" + entry.getKey() + "_" + tableName;
        File file = new File(filePath);
        if (file.exists()) {
          LoadChildThread loadChildThread = new LoadChildThread(tableName, entry.getKey(), filePath, updateFunc);
          Future<Boolean> future = MySQLBulkLoadExecutor.getInstance().submit(loadChildThread);
          futures.add(future);
        }
      }
    }

    for (Future<Boolean> booleanFuture : futures) {
      try {
        booleanFuture.get(120, TimeUnit.MINUTES);
      } catch (ExecutionException e) {
        LOG.error(e.getMessage(), e);
      } catch (TimeoutException e) {
        LOG.error(e.getMessage(), e);
        booleanFuture.cancel(true);
      }
    }

  }

  /**
   * 删掉数据文件。
   */
  private void clearTmpDataFiles(String filePath) throws Exception {
    Runtime rt = Runtime.getRuntime();
    String rmCmd = "rm " + filePath;
    String[] cmds = new String[]{"/bin/sh", "-c", rmCmd};
//    LOG.info("execShellCmd====" + rmCmd);
    execShellCmd(rt, cmds);
  }


  private Connection getUidConn(String dbName, long seqUid) throws SQLException {
    String nodeAddress = UidMappingUtil.getInstance().hash(seqUid);
    Connection conn = this.cons.get(nodeAddress);
    if (conn == null) {
      conn = MySql_16seqid.getInstance().getConnByNode(dbName, nodeAddress);
      this.cons.put(nodeAddress, conn);
    }
    return conn;
  }


  private Connection getNodeConn(String dbName, String nodeAddress) throws SQLException {
    Connection conn = this.cons.get(nodeAddress);
    if (conn == null) {
      conn = MySql_16seqid.getInstance().getConnByNode(dbName, nodeAddress);
      this.cons.put(nodeAddress, conn);
    }
    return conn;
  }

  private void execShellCmd(Runtime rt, String[] cmds) throws Exception {
    Process process = rt.exec(cmds);
    BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
    BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
    String cmdOutput = null;
    while ((cmdOutput = stdInput.readLine()) != null)
      LOG.warn(cmdOutput);
    while ((cmdOutput = stdError.readLine()) != null) {
      if (cmdOutput.contains("Can't connect to MySQL server on")) {
        LOG.error(cmdOutput);
      } else {
        cmdOutput = cmdOutput.replaceAll("ERROR", "e");
        LOG.warn(cmdOutput);
      }
    }
    int result = process.waitFor();
    if (result != 0)
      throw new RuntimeException("exec result not 0.");
    LOG.info("execShellCmd====" + cmds[2]);
  }

  class LoadChildThread implements Callable<Boolean> {

    private String tableName;
    private String nodeAddress;
    private String filePath;
    private UpdateFunc updateFunc;

    public LoadChildThread(String tableName, String nodeAddress, String filePath, UpdateFunc updateFunc) {
      this.tableName = tableName;
      this.nodeAddress = nodeAddress;
      this.filePath = filePath;
      this.updateFunc = updateFunc;
    }

    @Override
    public Boolean call() throws Exception {
      String onceOrCoverCmd = null;
      if (updateFunc == UpdateFunc.once) {
        onceOrCoverCmd = String.format("use 16_%s;LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE %s;",
                project, filePath, tableName);
      } else if (updateFunc == UpdateFunc.cover) {
        onceOrCoverCmd = String.format("use 16_%s;LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE %s;",
                project, filePath, tableName);
      }
      if (onceOrCoverCmd != null) {
        String cmd = String.format("mysql -h%s -u%s -p%s -e\"%s\"", nodeAddress, "xingyun", "Ohth3cha", onceOrCoverCmd);
        String[] cmds = new String[]{"/bin/sh", "-c", cmd};

        long currentTime = System.currentTimeMillis();

        while (true) {
          try {
            Runtime rt = Runtime.getRuntime();
            execShellCmd(rt, cmds);
            clearTmpDataFiles(filePath);
            break;
          } catch (InterruptedException ie) {
            break;
          } catch (Exception e) {
            LOG.warn(cmd + "\t" + e.getMessage());
            try {
              Thread.sleep(dead_lock_sleep_time);
            } catch (InterruptedException e1) {
              break;
            }
          }
        }


        LOG.info(project + "\t" + nodeAddress + "\t" + tableName + "\t using time:\t" + (System.currentTimeMillis() -
                currentTime) + "\tms.");
      }
      return true;
    }

  }
}
