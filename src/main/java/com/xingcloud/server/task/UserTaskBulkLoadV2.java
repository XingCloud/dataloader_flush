package com.xingcloud.server.task;

import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.mysql.UpdateFunc;
import com.xingcloud.mysql.UserProp;
import com.xingcloud.server.helper.Constants;
import com.xingcloud.server.helper.Pair;
import com.xingcloud.server.helper.ProjectPropertyCache;
import com.xingcloud.xa.uidmapping.UidMappingUtil;

import org.apache.commons.dbcp.DelegatingStatement;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;

/**
 * User: liuxiong
 * Date: 13-10-21
 * Time: 上午10:40
 */
public class UserTaskBulkLoadV2 implements Runnable {

  private static final Log LOG = LogFactory.getLog(UserTaskBulkLoadV2.class);

  private static final int MS_WHEN_SQL_EXCEPTION = 1 * 1000;

  private String project;
  private List<User_BulkLoad> users;
  private ProjectPropertyCache projectPropertyCache;


  public UserTaskBulkLoadV2(String project, List<User_BulkLoad> users) {
    this.project = project;
    this.users = users;
    // todo: load user properties from mysql each time?
    this.projectPropertyCache = ProjectPropertyCache.resetProjectCache(project);
  }


  @Override
  public void run() {
    try {
      LOG.info("enter user task. user size:" + users.size() + "\tproject: " + project);

      // (node, table) -> StringBuilder
      Map<Pair<String, String>, List<String>> nodeTableSBMap =
          new HashMap<Pair<String, String>, List<String>>();
      // node -> SQL list
      Map<String, List<String>> incSqls = new HashMap<String, List<String>>();

      long startTime = System.currentTimeMillis();

      prepareUsers(incSqls, nodeTableSBMap);

      LOG.info("USerTask_BulkLoad==== " + project + " prepareUsers using time: "
        + (System.currentTimeMillis() - startTime) + "ms. size:" + users.size());

      startTime = System.currentTimeMillis();

      bulkLoad(nodeTableSBMap);

      LOG.info("USerTask_BulkLoad==== " + project + " bulkLoad using time: "
        + (System.currentTimeMillis() - startTime) + "ms. size:" + users.size());

      startTime = System.currentTimeMillis();

      incSqlsLoadToMySQL(incSqls);

      LOG.info("USerTask_BulkLoad==== " + project + " incSqlsLoadToMySQL using time: "
        + (System.currentTimeMillis() - startTime) + "ms. size:" + users.size());
    } catch (Exception e) {
      LOG.error("end this thread.", e);
    }
  }


  /**
   * 分析user.log，生成每个node对应的load data文件
   *
   * @param incSqls
   * @param nodeTableSBMap
   */
  private void prepareUsers(Map<String, List<String>> incSqls,
                            Map<Pair<String, String>, List<String>> nodeTableSBMap) {
    // sort List<User_BulkLoad> users, in user hash uid order.
    Collections.sort(users, new UserComparator());

    for (User_BulkLoad user : users) {
      String nodeAddress = UidMappingUtil.getInstance().hash(user.getSeqUid());

      List<String> propKeys = user.getPropKeys();
      List<String> propValues = user.getPropValues();

      for (int i = 0; i < propKeys.size(); i++) {
        String key = propKeys.get(i); // table name
        String value = propValues.get(i); // row value

        UserProp userProp = projectPropertyCache.getUserPro(key);
        if (userProp == null) {
          LOG.error("user property is null for key: " + key + ", project: " + project);
          continue;
        }

        if (userProp.getPropFunc() == UpdateFunc.inc) {
          String incSql = "INSERT INTO `" + key + "` (uid,val) VALUES (" + user.getSamplingUid() + "," +
                  value + ") ON DUPLICATE KEY UPDATE val=val+" + value + ";";

          List<String> sqls = incSqls.get(nodeAddress);
          if (sqls == null) {
            sqls = new ArrayList<String>();
            incSqls.put(nodeAddress, sqls);
          }
          sqls.add(incSql);
        } else {
          Pair<String, String> nodeTablePair = new Pair<String, String>(nodeAddress, key);
          List<String> sbs = nodeTableSBMap.get(nodeTablePair);
          if (sbs == null) {
            sbs = new ArrayList<String>();
            nodeTableSBMap.put(nodeTablePair, sbs);
          }

//          sb.append(user.getSamplingUid()).append("\t").append(value).append("\n");
          // csv format
//          sb.append(user.getSamplingUid()).append(",").append(StringEscapeUtils.escapeCsv(value)).append("\n");
            sbs.add(user.getSamplingUid() + "," + StringEscapeUtils.escapeCsv(value)+ "\n");
        }
      }
    }
  }

  /**
   * inc的属性不能通过load data，还是通过mysql connection statement。
   */
  private void incSqlsLoadToMySQL(Map<String, List<String>> incSqls) throws InterruptedException {
    Connection connection = null;
    Statement statement = null;

    for (Map.Entry<String, List<String>> entry : incSqls.entrySet()) {
      // todo: the following sort may take much time.
      // in prepareUsers method, already sorted according to sample uid, why sort again?
      // to group insert statements by table?
      Collections.sort(entry.getValue());
      LOG.info("project: " + project + ", node: " + entry.getKey() + ", insert sql count: " + entry.getValue().size());

      boolean successful = false;
      int tryTimes = 1;
      while (!successful) {
        try {
          // for each retry, initialize connection to null
          connection = null;
          connection = getNodeConn(project, entry.getKey());
          connection.setAutoCommit(false);

          statement = connection.createStatement();
          // currently, insert sql count is less than 100
          statement.clearBatch();
          for (String sql : entry.getValue()) {
            statement.addBatch(sql);
          }
          statement.executeBatch();

          connection.commit();
          successful = true;
        } catch (SQLException sqlex) {
          LOG.error("inc sql failed. " + project + " " + entry.getKey() +
            " retry in " + MS_WHEN_SQL_EXCEPTION * tryTimes / 1000 +
            " seconds." + sqlex.getMessage());

          if (connection != null) {
            try {
              connection.rollback();
            } catch (SQLException sqlexception) {
              LOG.error(sqlexception.getMessage());
            }
          }
        } finally {
          DbUtils.closeQuietly(statement);
          DbUtils.closeQuietly(connection);
        }
        successful = true;
        if (!successful) {
          Thread.sleep(MS_WHEN_SQL_EXCEPTION * tryTimes);
          tryTimes = (tryTimes << 1) & Integer.MAX_VALUE;
        }
      }
    }
  }


  /**
   * bulk load，机器和每个机器对应的表 顺序 乱序，尽量减少出现2个进程同时load data一个表，会发生deadlock（数据不会丢，程序会重试）。
   *
   * @param nodeTableSBMap
   */
  private void bulkLoad(Map<Pair<String, String>, List<String>> nodeTableSBMap) throws InterruptedException {
    List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();

    for (Map.Entry<Pair<String, String>, List<String>> entry: nodeTableSBMap.entrySet()) {
      Pair<String, String> nodeTablePair = entry.getKey();

      UpdateFunc updateFunc = projectPropertyCache.getUserPro(nodeTablePair.second).getPropFunc();
      if (updateFunc == null) {
        LOG.warn("updateFunc is null. " + "project: " + project + ". " + nodeTablePair);
        continue;
      }

      LoadChildThread loadChildThread = new LoadChildThread(
        nodeTablePair.first, nodeTablePair.second, entry.getValue(), updateFunc);

      Future<Boolean> future = MySQLBulkLoadExecutor.getInstance().submit(loadChildThread);
      futures.add(future);
    }

    for (Future<Boolean> booleanFuture : futures) {
      try {
        booleanFuture.get(240, TimeUnit.MINUTES);
      } catch (ExecutionException e) {
        LOG.error(e.getMessage(), e);
      } catch (TimeoutException e) {
        LOG.error(e.getMessage(), e);
        booleanFuture.cancel(true);
      }
    }
  }


  private Connection getNodeConn(String project, String nodeAddress) throws SQLException {
    return MySql_16seqid.getInstance().getConnByNode(project, nodeAddress);
  }

  class LoadChildThread implements Callable<Boolean> {

    private String nodeAddress;
    private String tableName;
    private List<String> loadDatas;
    private UpdateFunc updateFunc;

    public LoadChildThread(String nodeAddress, String tableName, List<String> loadDatas, UpdateFunc updateFunc) {
      this.nodeAddress = nodeAddress;
      this.tableName = tableName;
      this.loadDatas = loadDatas;
      this.updateFunc = updateFunc;
    }

    @Override
    public String toString() {
      return "project: " + project + ", node: " + nodeAddress + ", table: " + tableName + ", updateFunc: " + updateFunc;
    }

    @Override
    public Boolean call() throws Exception {
      String loadDataSQL = null;
      if (updateFunc == UpdateFunc.once) {
        loadDataSQL = "load data local infile 'ignore_me' " +
                      " ignore into table " + tableName +
                      " character set utf8 " +
                      " fields terminated by ',' optionally enclosed by '\"' escaped by '\"'";
//        loadDataSQL = String.format("LOAD DATA LOCAL INFILE 'ignore_me' IGNORE INTO TABLE %s;", tableName);
      } else if (updateFunc == UpdateFunc.cover) {
        loadDataSQL = "load data local infile 'ignore_me' " +
                      " replace into table " + tableName +
                      " character set utf8 " +
                      " fields terminated by ',' optionally enclosed by '\"' escaped by '\"'";
//        loadDataSQL = String.format("LOAD DATA LOCAL INFILE 'ignore_me' REPLACE INTO TABLE %s;", tableName);
      }

      if (loadDataSQL != null) {
        Connection loadDataConnection = null;
        com.mysql.jdbc.Statement loadDataStatement = null;

        long startTime = System.currentTimeMillis();

          List<StringBuilder> sbs =  new ArrayList<StringBuilder>();

          int count = 0;
          StringBuilder sb = null;

          for(String data : loadDatas){
              if(count % 20000 == 0){ //每个表分批入库，少量多次，减轻mysql压力，避免出现某些导入耗费时间太久，导致整体导入缓慢
                  sb = new StringBuilder();
                  sbs.add(sb);
              }
              sb.append(data);
              count++;
          }
          for(StringBuilder loadData : sbs ){
        int tryTimes = 1;
        boolean successful = false;

            while (!successful) {
              try {
                // for each retry, initialize connection to null
                loadDataConnection = null;
                loadDataConnection = getNodeConn(project, nodeAddress);
                loadDataConnection.setAutoCommit(false);

                Statement statement = loadDataConnection.createStatement();
                statement = ((DelegatingStatement)statement).getInnermostDelegate();

                assert statement != null && statement instanceof com.mysql.jdbc.Statement;

                loadDataStatement = (com.mysql.jdbc.Statement)statement;
                // by default, mysql jdbc driver sets sql_mode to STRICT_TRANS_TABLES
                // by setting sql_mode to none, we disable data truncation exception
                loadDataStatement.execute("set sql_mode=''");
                loadDataStatement.setLocalInfileInputStream(IOUtils.toInputStream(loadData.toString(), Charsets.UTF_8));
                loadDataStatement.execute(loadDataSQL);
                loadDataConnection.commit();

                successful = true;
              } catch (SQLException sqle) {
                LOG.error("load data failed. " + toString() +
                  " retry load data in " + MS_WHEN_SQL_EXCEPTION * tryTimes / 1000 +
                  " seconds." + sqle.getMessage());

                if (loadDataConnection != null) {
                  try {
                    loadDataConnection.rollback();
                  } catch (SQLException sqlexception) {
                    LOG.error(sqlexception.getMessage());
                  }
                }
              } finally {
                DbUtils.closeQuietly(loadDataStatement);
                DbUtils.closeQuietly(loadDataConnection);
              }

              if (!successful) {
                try {
                  Thread.sleep(MS_WHEN_SQL_EXCEPTION * tryTimes);
                  tryTimes = (tryTimes << 1) & Integer.MAX_VALUE;
                    if(tryTimes > 1024){  //最多等待10多分钟
                        tryTimes = 1024;
                    }
                } catch (InterruptedException ie1) {
                  successful = true;
                }
              }
            }
          }

        LOG.info(toString() + "\tcost time:\t" + (System.currentTimeMillis() - startTime) + "ms.\tcount:\t" + count);
      }

      return true;
    }
  }
}
