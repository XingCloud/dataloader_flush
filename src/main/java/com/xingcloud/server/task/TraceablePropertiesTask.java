package com.xingcloud.server.task;

import com.xingcloud.server.helper.Constants;
import com.xingcloud.server.helper.ProjectPropertyCacheInHBase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * User: IvyTang
 * Date: 13-7-5
 * Time: PM7:01
 */
public class TraceablePropertiesTask implements Runnable {

  private static final Log LOG = LogFactory.getLog(TraceablePropertiesTask.class);


  private Map<String, Map<String, Map<String, List<Put>>>> traceableProperties;

  public TraceablePropertiesTask(Map<String, Map<String, Map<String, List<Put>>>> traceableProperties) {
    this.traceableProperties = traceableProperties;
  }

  @Override
  public void run() {
    for (Map.Entry<String, Map<String, Map<String, List<Put>>>> entry : traceableProperties.entrySet()) {
      LOG.info(entry.getKey());
      Configuration configuration = HBaseConf.getInstance().getHBaseConf(entry.getKey());
      for (Map.Entry<String, Map<String, List<Put>>> pEntry : entry.getValue().entrySet()) {
        String pid = pEntry.getKey();
        for (Map.Entry<String, List<Put>> propertyEntry : pEntry.getValue().entrySet()) {
          HTable hTable = null;
          try {
            hTable = new HTable(configuration, getTableName(pid, propertyEntry.getKey()));
            hTable.setAutoFlush(false);
            hTable.setWriteBufferSize(Constants.WRITE_BUFFER_SIZE);
            hTable.put(propertyEntry.getValue());
            hTable.flushCommits();
          } catch (IOException e) {
            LOG.error("TraceablePropertiesTask run error", e);
          } finally {
            if (hTable != null)
              try {
                hTable.close();
              } catch (IOException e) {
                LOG.error(e.getMessage());
              }
          }
        }
      }
    }
  }

  public static String getTableName(String project, String propertyName) {
    return "property_" + project + "_" + ProjectPropertyCacheInHBase.getInstance().getPropertyID(project, propertyName);
  }
}