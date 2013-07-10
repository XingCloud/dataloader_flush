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
  private String hbaseNodeAddress;
  private List<Put> userProperties;

  public HBasePropertiesTask(String project, String hbaseNodeAddress, List<Put> userProperties) {
    this.project = project;
    this.hbaseNodeAddress = hbaseNodeAddress;
    this.userProperties = userProperties;
  }

  @Override
  public void run() {
//    System.out.println(project + "\t" + hbaseNodeAddress + "\t" + userProperties.size());
    Configuration configuration = HBaseKeychain.getInstance().getConfigs().get(0).configs().get("192.168.1.25");

//      Configuration configuration =  HBaseKeychain.getInstance().getConfigs().get(0).configs().get(hbaseNodeAddress);

    HTable hTable = null;
    try {
      hTable = new HTable(configuration, "properties_" + project);
      hTable.setAutoFlush(false);
      hTable.setWriteBufferSize(Constants.WRITE_BUFFER_SIZE);
      hTable.put(userProperties);
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