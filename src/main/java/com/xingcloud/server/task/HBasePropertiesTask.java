package com.xingcloud.server.task;

import com.xingcloud.server.helper.Constants;
import com.xingcloud.server.helper.ProjectPropertyCacheInHBase;
import com.xingcloud.userprops_meta_util.hbasehash.HBaseKeychain;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * User: IvyTang
 * Date: 13-7-5
 * Time: PM7:01
 */
public class HBasePropertiesTask implements Runnable {

  private static final Log LOG = LogFactory.getLog(HBasePropertiesTask.class);


  private String project;
  private Map<String, Map<String, List<Row>>> hbaseProperties;

  public HBasePropertiesTask(String project, Map<String, Map<String, List<Row>>> hbaseProperties) {
    this.project = project;
    this.hbaseProperties = hbaseProperties;
  }

  @Override
  public void run() {
    for (Map.Entry<String, Map<String, List<Row>>> entry : hbaseProperties.entrySet()) {
      Configuration configuration = HBaseKeychain.getInstance().getConfigs().get(0).configs().get("192.168.1.25");
//      Configuration configuration =  HBaseKeychain.getInstance().getConfigs().get(0).configs().get(entry.getKey());
      for (Map.Entry<String, List<Row>> propEntry : entry.getValue().entrySet()) {
        HTable hTable = null;
        try {
          hTable = new HTable(configuration, getTableName(project, propEntry.getKey()));
          hTable.setAutoFlush(false);
          hTable.setWriteBufferSize(Constants.WRITE_BUFFER_SIZE);
          hTable.batch(propEntry.getValue());
          hTable.flushCommits();
        } catch (Exception e) {
          LOG.error("HBasePropertiesTask error.", e);
        } finally {
          if (hTable != null)
            try {
              hTable.close();
            } catch (IOException e) {
              LOG.error(e.getMessage(), e);
            }
        }
      }
    }
  }

  public static String getTableName(String project, String propertyName) {
    return "property_" + project + "_" + ProjectPropertyCacheInHBase.getInstance().getPropertyID(project, propertyName);
  }
}