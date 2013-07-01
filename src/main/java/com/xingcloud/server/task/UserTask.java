package com.xingcloud.server.task;

import com.xingcloud.mysql.MySql_fixseqid;
import com.xingcloud.mysql.MySql_seqid;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: IvyTang
 * Date: 12-11-28
 * Time: 下午4:43
 */
public class UserTask implements Runnable {


  private static final Log LOG = LogFactory.getLog(UserTask.class);

  private String project;
  private List<User> users;
  private Map<String, Connection> cons = null;

  public UserTask(String project, List<User> users) {
    this.project = project;
    this.users = users;
    this.cons = new HashMap<String, Connection>();
  }

  @Override
  public void run() {
    LOG.info("enter user task:user size:" + users.size() + "\t" + project);
    Map<String, Long> perNodeSize = new HashMap<String, Long>();
    Map<String, Long> perNodeUsingTime = new HashMap<String, Long>();
    Map<String, Long> perNodeBitmapCachedSize = new HashMap<String, Long>();
    Map<String, Long> perNodeReallyNeedColdToHotSize = new HashMap<String, Long>();
    Map<String, Long> perNodeColdHotRemoteUsingTime = new HashMap<String, Long>();
    long currentTime = System.currentTimeMillis();
    try {
      for (User user : users) {
        long userCurrentTime = System.currentTimeMillis();
        boolean putMysql = true;
        while (putMysql) {
          Connection connection = null;
          Statement stat = null;
          try {
            String userNode = UidMappingUtil.getInstance().hash(user.getSeqUid());
            connection = getUidConn(project, user.getSeqUid());

            //check in local cache
            boolean inLocalHotCache = HotUserProjectBitmap.getInstance().ifInLocalCacheHot(this.project, user.getSeqUid());
            if (inLocalHotCache) {
              staticsMap(perNodeBitmapCachedSize, userNode, 1l);
            } else {
              long userRemoteColdHotCheckTimeBegin = System.currentTimeMillis();
              boolean inRemoteHot = checkInHot(connection, user);
              if (!inRemoteHot) {
                cold2Hot(user);
                staticsMap(perNodeReallyNeedColdToHotSize, userNode, 1l);
              }
              HotUserProjectBitmap.getInstance().markLocalCacheHot(this.project, user.getSeqUid());
              staticsMap(perNodeColdHotRemoteUsingTime, userNode, System.currentTimeMillis() - userRemoteColdHotCheckTimeBegin);
            }

            connection.setAutoCommit(false);//以下mysql更新用批量
            stat = connection.createStatement();
            for (String tempSql : user.getSqlProperties()) {
              stat.addBatch(tempSql);
            }
            stat.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);//恢复connection默认方式
            putMysql = false;

            staticsMap(perNodeSize, userNode, 1l);
            staticsMap(perNodeUsingTime, userNode, System.currentTimeMillis() - userCurrentTime);

          } catch (SQLException e) {
            if (e.getMessage().startsWith("You have an error in your SQL syntax")) {
              LOG.warn("SQL syntax " + user.toString());
              putMysql = false;
            } else {
              LOG.error(project + " put mysql error." + e.getMessage(), e);
              LOG.error(user.toString());
              LOG.info(project + " trying put mysql  again.");
              putMysql = true;
              try {
                if (stat != null) {
                  stat.close();
                  stat = null;
                }
                if (connection != null) {
                  cons.remove(UidMappingUtil.getInstance().hash(user.getSeqUid()));
                  connection.close();
                }
              } catch (SQLException e1) {
                LOG.error(e1.getMessage());
              }
            }
          } finally {
            try {
              if (stat != null)
                stat.close();
            } catch (SQLException e) {
              LOG.error(e.getMessage());
            }
          }
        }
      }
    } finally {
      try {
        for (Map.Entry<String, Connection> entry : cons.entrySet()) {
          if (entry.getValue() != null)
            entry.getValue().close();
        }
      } catch (SQLException e) {
        LOG.error(e.getMessage());
      }

    }
    for (Map.Entry<String, Long> entry : perNodeSize.entrySet()) {
      long nodeBitmapCachedSize = perNodeBitmapCachedSize.get(entry.getKey()) == null ? 0 : perNodeBitmapCachedSize.get
              (entry.getKey());
      long nodeReallyNeedColdToHotSize = perNodeReallyNeedColdToHotSize.get(entry.getKey()) ==
              null ? 0 : perNodeReallyNeedColdToHotSize.get(entry.getKey());
      long nodeColdHotRemoteUsingTime = perNodeColdHotRemoteUsingTime.get(entry.getKey()) ==
              null ? 0 : perNodeColdHotRemoteUsingTime.get(entry.getKey());
      LOG.info(project + " " + entry.getKey() + " size: " + entry.getValue() +
              " using: " + perNodeUsingTime.get(entry.getKey()) +
              " bitmap cached: " + nodeBitmapCachedSize +
              " remote mysql really need htc size: " + nodeReallyNeedColdToHotSize +
              " remote mysql htc time: " + nodeColdHotRemoteUsingTime);
    }
    LOG.info(project + "\tput mysql users size:" + users.size() + ".using "
            + (System.currentTimeMillis() - currentTime) + "ms" + ".");
  }

  private boolean checkInHot(Connection conn, User user) throws SQLException {
    Statement stat = null;
    ResultSet resultSet = null;
    try {
      stat = conn.createStatement();
      resultSet = stat.executeQuery("select uid from `register_time` where uid=" + user.getSamplingUid() + ";");
      return resultSet.next();
    } finally {
      if (resultSet != null)
        resultSet.close();
      if (stat != null)
        stat.close();
    }
  }

  private void staticsMap(Map<String, Long> map, String key, Long value) {
    Long mapValue = map.get(key);
    if (mapValue == null)
      mapValue = 0l;
    mapValue += value;
    map.put(key, mapValue);
  }


  private void cold2Hot(User user) throws SQLException {
    MySql_fixseqid.getInstance().cold2hot(project, user.getSamplingUid());
  }

  private Connection getUidConn(String dbName, long seqUid) throws SQLException {
    String nodeAddress = UidMappingUtil.getInstance().hash(seqUid);
    Connection conn = this.cons.get(nodeAddress);
    if (conn == null) {
      conn = MySql_fixseqid.getInstance().getConnByNode(dbName, nodeAddress);
      this.cons.put(nodeAddress, conn);
    }
    return conn;
  }


}
