package com.xingcloud.server.tailler;

import com.xingcloud.server.helper.Constants;
import com.xingcloud.server.helper.Helper;
import com.xingcloud.server.helper.ProjectPropertyCacheInHBase;
import com.xingcloud.server.task.FlushExecutor;
import com.xingcloud.server.task.HBasePropertiesTask;
import com.xingcloud.userprops_meta_util.UpdateFunc;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * User: IvyTang
 * Date: 13-7-8
 * Time: PM3:28
 */
public class HBaseUPTailer extends Tail {

  private static final Log LOG = LogFactory.getLog(HBaseUPTailer.class);

  public HBaseUPTailer(String configPath) {
    super(configPath);
    setBatchSize(Constants.HBASEUP_ONCE_READ);
    setLogProcessPerBatch(Constants.WRITE_SENDPROCESS_PER_BATCH);
    LOG.info(configPath);
    LOG.info(this.datafile);
    LOG.info(this.day);
  }

  @Override
  public void send(List<String> logs, long day) {
    LOG.info("======HBaseUPTailer=======" + day + " events log ..." + logs.size());
    long currentTime = System.currentTimeMillis();
    try {
      FlushExecutor eventExecutor = new FlushExecutor();
      Map<String, Map<String, Map<String, List<Row>>>> userProperties = analysisUserUP(logs);
      for (Map.Entry<String, Map<String, Map<String, List<Row>>>> entry : userProperties.entrySet()) {
        HBasePropertiesTask hBasePropertiesTask = new HBasePropertiesTask(entry.getKey(), entry.getValue());
        eventExecutor.execute(hBasePropertiesTask);
      }
      eventExecutor.shutdown();
      boolean result = eventExecutor.awaitTermination(Constants.EXECUTOR_TIME_MIN, TimeUnit.MINUTES);
      if (!result) {
        LOG.warn("HBaseUPTailerExecutor timeout....throws this exception to tailer and quit this.");
        eventExecutor.shutdownNow();
        throw new RuntimeException("HBaseUPTailerExecutor timeout.");
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
    LOG.info("======HBaseUPTailer=======" + day + " events log send completed." + logs.size() +
            " using " + (System.currentTimeMillis() - currentTime) + "ms.");
  }

  private Map<String, Map<String, Map<String, List<Row>>>> analysisUserUP(List<String> logs) {
    ObjectMapper objectMapper = new ObjectMapper();
    //Map<Pid,Map<hbaseNode,Map<userProperties,List<Row>>>
    Map<String, Map<String, Map<String, List<Row>>>> hbaseUPs = new HashMap<String, Map<String, Map<String,
            List<Row>>>>();

    for (String log : logs) {
      String[] tmps = log.split("\t");
      if (tmps.length != Constants.USER_ITEM_NUM) {
        LOG.warn(log);
        continue;

      }
      String pid = tmps[0];
      long samplingUid = UidMappingUtil.getInstance().decorateWithMD5(Long.valueOf(tmps[1]));
      try {
        Map jsonMap = objectMapper.readValue(tmps[2], Map.class);
        for (Object entry : jsonMap.entrySet()) {
          if (entry instanceof Map.Entry) {
            String key = ((Map.Entry) entry).getKey().toString();
            String value = ((Map.Entry) entry).getValue().toString();
            if (ProjectPropertyCacheInHBase.getInstance().getPropertyID(tmps[0], key) > Constants.NULL_MAXPROPERTYID) {
              String nodeAddress = UidMappingUtil.getInstance().hash(Long.valueOf(tmps[1]));

              //得到该项目的Map<String,Map<String,List<Row>>>
              Map<String, Map<String, List<Row>>> pHBaseUps = hbaseUPs.get(pid);
              if (pHBaseUps == null) {
                pHBaseUps = new HashMap<String, Map<String, List<Row>>>();
                hbaseUPs.put(pid, pHBaseUps);
              }

              //得到对应节点的  Map<String, List<Row>>
              Map<String, List<Row>> nodePuts = pHBaseUps.get(nodeAddress);
              if (nodePuts == null) {
                nodePuts = new HashMap<String, List<Row>>();
                pHBaseUps.put(nodeAddress, nodePuts);
              }

              //某个属性的List<Row>
              List<Row> rows = nodePuts.get(key);
              if (rows == null) {
                rows = new ArrayList<Row>();
                nodePuts.put(key, rows);
              }

              byte[] uidBytes = Bytes.toBytes(samplingUid);
              byte[] shortenUid = {uidBytes[3], uidBytes[4], uidBytes[5], uidBytes[6], uidBytes[7]};

              UpdateFunc upUpdateFunc = ProjectPropertyCacheInHBase.getInstance().getPropertyFunc(tmps[0], key);
              if (upUpdateFunc == UpdateFunc.once) {
                Put put = new Put(shortenUid);
                put.add(Bytes.toBytes(Constants.UP_COLUMNFAMILY), Bytes.toBytes(Constants.UP_COLUMNFAMILY),
                        Helper.transformOnceTimestamp(), Bytes.toBytes(value));
                put.setDurability(Durability.SKIP_WAL);
                rows.add(put);
              } else if (upUpdateFunc == UpdateFunc.cover) {
                Put put = new Put(shortenUid);
                put.add(Bytes.toBytes(Constants.UP_COLUMNFAMILY), Bytes.toBytes(Constants.UP_COLUMNFAMILY),
                        Helper.getCurrentDayBeginTimestamp(), Bytes.toBytes(value));
                put.setDurability(Durability.SKIP_WAL);
                rows.add(put);
              } else if (upUpdateFunc == UpdateFunc.inc) {
                Increment increment = new Increment(shortenUid);
                increment.addColumn(Bytes.toBytes(Constants.UP_COLUMNFAMILY), Bytes.toBytes(Constants.UP_COLUMNFAMILY),
                        Long.parseLong(value));
                increment.setDurability(Durability.SKIP_WAL);
                rows.add(increment);
              }
            }
          }
        }
      } catch (IOException e) {
        LOG.warn("json parse error." + e.getMessage());
        LOG.warn(log);
      }

    }
    return hbaseUPs;
  }


}
