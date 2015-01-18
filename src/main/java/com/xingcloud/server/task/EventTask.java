package com.xingcloud.server.task;

import com.xingcloud.server.helper.Constants;
import com.xingcloud.server.helper.Helper;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * User: IvyTang
 * Date: 12-11-29
 * Time: 上午10:54
 */
public class EventTask implements Runnable {


  private static final Log LOG = LogFactory.getLog(EventTask.class);

  private String project;
  private List<Event> events;

  private Map<String, List<Put>> putsMap;

  public EventTask(String project, List<Event> events) {
    this.project = project;
    this.events = events;
    this.putsMap = new HashMap<String, List<Put>>();
  }

  @Override
  public void run() {
    LOG.info("enter run event task. " + project + " events size:" + events.size());
    try {
      for (Event event : events) {
        String hbaseAddress = UidMappingUtil.getInstance().hash(event.getSeqUid());
        Put put = new Put(event.getRowKey());
        put.setWriteToWAL(Constants.deuTableWalSwitch);
        put.add(Constants.columnFamily.getBytes(), Constants.columnFamily.getBytes(), event.getTimeStamp(),
                Bytes.toBytes(event.getValue()));
        List<Put> puts = putsMap.get(hbaseAddress);
        if (puts == null) {
          puts = new ArrayList<Put>();
          putsMap.put(hbaseAddress, puts);
        }
        puts.add(put);

      }
      EventFlushExecutor flushExecutor = new EventFlushExecutor();
      for (Map.Entry<String, List<Put>> entry : putsMap.entrySet()) {
        EventFlushChildTask childTask = new EventFlushChildTask(project, entry.getKey(), entry.getValue());
        flushExecutor.execute(childTask);
      }
      flushExecutor.shutdown();


      try {
        if (!flushExecutor.awaitTermination(Constants.MYSQLBL_TIME_MIN,TimeUnit.HOURS)) {     //不能让childThread出现超时的情况，只能由userExecutor发起 shutdownNow。
          flushExecutor.shutdownNow();
          throw new RuntimeException("EventFlushChildTask timeout.");
        }
      } catch (InterruptedException e) {
        //如果mysql bulk load child thread没有完成，但是userExecutor已经超时，userExecutor执行shutdownnow操作，会使mySQLBulkLoadExecutor
        //抛错InterruptedException，这时候需要再 mySQLBulkLoadExecutor.shutdownNow()
        //让mySQLBulkLoadExecutor的子线程也抛错；否则mySQLBulkLoadExecutor的子线程会成为僵尸线程。
        flushExecutor.shutdownNow();
        throw e;
      }

    } catch (Exception e) {
      LOG.error(project + e.getMessage(), e);
    }
  }


}

class EventFlushChildTask implements Runnable {

  private static final Log LOG = LogFactory.getLog(EventFlushChildTask.class);

  private String project;
  private String hbaseip;
  private List<Put> puts;


  public EventFlushChildTask(String project, String hbaseip, List<Put> puts) {
    this.project = project;
    this.hbaseip = hbaseip;
    this.puts = puts;
  }


  @Override
  public void run() {

    while (true) {
      HTable table = null;
      long currentTime = System.currentTimeMillis();
      try {

        // todo: use connection pool?

        table = new HTable(HBaseConf.getInstance().getHBaseConf(hbaseip),
                Helper.getHBaseTableName(project));
//        LOG.info(project + hbaseip + " init htable .." + currentTime);
        table.setAutoFlush(false);
        table.setWriteBufferSize(Constants.WRITE_BUFFER_SIZE);

        table.put(puts);
        table.flushCommits();

        LOG.info(project + " " + hbaseip + " put hbase size:" + puts.size() +
                " completed .tablename is " + Helper.getHBaseTableName(project) + " using "
                + (System.currentTimeMillis() - currentTime) + "ms");
        break;
      } catch (Exception e) {
        break;
        /*if (e.getMessage().contains("interrupted")) {
          break;
        }
        LOG.error(project + hbaseip + e.getMessage(), e);
        if (e.getMessage().contains("HConnectionImplementation") && e.getMessage().contains("closed")) {
          HConnectionManager.deleteConnection(HBaseConf.getInstance().getHBaseConf(hbaseip), true);
          LOG.warn("delete connection to " + hbaseip);
        }

        LOG.info("trying put hbase " + project + " " + hbaseip + "again...tablename " +
                ":" + Helper.getHBaseTableName(project));
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e1) {
          break;
        }*/
      } finally {
        try {
          if (table != null) {
            table.close();
//            LOG.info(project + " close this htable." + currentTime);
          }
        } catch (IOException e) {
          LOG.error(project + e.getMessage(), e);
        }
      }
    }
  }
}
