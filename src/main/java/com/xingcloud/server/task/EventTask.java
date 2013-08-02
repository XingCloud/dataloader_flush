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
            for (Map.Entry<String, List<Put>> entry : putsMap.entrySet()) {
                boolean putHbase = true;
                while (putHbase) {
                    HTable table = null;
                    long currentTime = System.currentTimeMillis();
                    try {

                        table = new HTable(HBaseConf.getInstance().getHBaseConf(entry.getKey()),
                                Helper.getHBaseTableName(project));
                        LOG.info(project + " init htable .." + currentTime);
                        table.setAutoFlush(false);
                        table.setWriteBufferSize(Constants.WRITE_BUFFER_SIZE);

                        table.put(entry.getValue());
                        table.flushCommits();

                        putHbase = false;
                        LOG.info(project + " " + entry.getKey() + " put hbase size:" + entry.getValue().size() +
                                " completed .tablename is " + Helper.getHBaseTableName(project) + " using "
                                + (System.currentTimeMillis() - currentTime) + "ms");
                    } catch (IOException e) {
                        if (e.getMessage().contains("interrupted")) {
                            throw e;
                        }
                        LOG.error(project + entry.getKey() + e.getMessage(), e);
                        if (e.getMessage().contains("HConnectionImplementation") && e.getMessage().contains("closed")) {
                            HConnectionManager.deleteConnection(HBaseConf.getInstance().getHBaseConf(entry.getKey()), true);
                            LOG.warn("delete connection to " + entry.getKey());
                        }
                        putHbase = true;

                        LOG.info("trying put hbase " + project + " " + entry.getKey() + "again...tablename " +
                                ":" + Helper.getHBaseTableName(project));
                        Thread.sleep(5000);
                    } finally {
                        try {
                            if (table != null) {
                                table.close();
                                LOG.info(project + " close this htable." + currentTime);
                            }
                        } catch (IOException e) {
                            LOG.error(project + e.getMessage(), e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(project + e.getMessage(), e);
        }
    }


}
