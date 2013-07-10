package com.xingcloud.server.tailler;

import com.xingcloud.server.helper.Constants;
import com.xingcloud.server.helper.Helper;
import com.xingcloud.server.helper.ProjectPropertyCacheInHBase;
import com.xingcloud.server.task.FlushExecutor;
import com.xingcloud.server.task.HBasePropertiesTask;
import com.xingcloud.userprops_meta_util.Base64Util_Helper;
import com.xingcloud.userprops_meta_util.UpdateFunc;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Durability;

import org.apache.hadoop.hbase.client.Put;

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
      Map<String, Map<String, List<Put>>> userProperties = analysisUserUP(logs);
      for (Map.Entry<String, Map<String, List<Put>>> entry : userProperties.entrySet()) {
        System.out.println(entry.getKey());
        for (Map.Entry<String, List<Put>> pEntry : entry.getValue().entrySet()) {
          System.out.println("\t" + pEntry.getKey());
          for (Put put : pEntry.getValue()) {

            byte[] uids = new byte[]{put.getRow()[1], put.getRow()[2], put.getRow()[3],
                    put.getRow()[4]};
            System.out.println("\t\t" + Bytes.toInt(uids) + "\t" + put.toJSON());

          }
        }
      }
      //每个 HBasePropertiesTask处理的put数<=  Constants.HBASEUP_ONE_THREAD_PUT
      for (Map.Entry<String, Map<String, List<Put>>> entry : userProperties.entrySet()) {
        for (Map.Entry<String, List<Put>> pEntry : entry.getValue().entrySet()) {
          if (pEntry.getValue().size() <= Constants.HBASEUP_ONE_THREAD_PUT) {
            HBasePropertiesTask hBasePropertiesTask = new HBasePropertiesTask(entry.getKey(), pEntry.getKey(),
                    pEntry.getValue());
            eventExecutor.execute(hBasePropertiesTask);
          } else {
            int sub_task_num = pEntry.getValue().size() / Constants.HBASEUP_ONE_THREAD_PUT + (pEntry.getValue().size()
                    % Constants.HBASEUP_ONE_THREAD_PUT == 0 ? 0 : 1);
            for (int i = 0; i < sub_task_num; i++) {
              HBasePropertiesTask hBasePropertiesTask = null;
              if (i == sub_task_num - 1) {
                hBasePropertiesTask = new HBasePropertiesTask(entry.getKey(), pEntry.getKey(),
                        pEntry.getValue().subList(i * Constants.HBASEUP_ONE_THREAD_PUT, pEntry.getValue().size()));
              } else {
                hBasePropertiesTask = new HBasePropertiesTask(entry.getKey(), pEntry.getKey(),
                        pEntry.getValue().subList(i * Constants.HBASEUP_ONE_THREAD_PUT, (i + 1) * Constants.HBASEUP_ONE_THREAD_PUT));
              }
              eventExecutor.execute(hBasePropertiesTask);
            }
          }
        }
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

  private Map<String, Map<String, List<Put>>> analysisUserUP(List<String> logs) {
    ObjectMapper objectMapper = new ObjectMapper();

    Map<String, Map<String, List<Put>>> hbaseUPs = new HashMap<String, Map<String, List<Put>>>();

    for (String log : logs) {
      String[] tmps = log.split("\t");
      if (tmps.length != Constants.USER_ITEM_NUM) {
        LOG.warn(log);
        continue;
      }
      String pid = tmps[0];
      long samplingUid = UidMappingUtil.getInstance().decorateWithMD5(Long.valueOf(tmps[1]));

      byte[] uidBytes = Bytes.toBytes(samplingUid);
      byte[] shortenUid = {uidBytes[3], uidBytes[4], uidBytes[5], uidBytes[6], uidBytes[7]};
      try {
        Map jsonMap = objectMapper.readValue(tmps[2], Map.class);

        //得到该项目的Map<String,List<Put>>
        Map<String, List<Put>> pHBaseUps = hbaseUPs.get(pid);
        if (pHBaseUps == null) {
          pHBaseUps = new HashMap<String, List<Put>>();
          hbaseUPs.put(pid, pHBaseUps);
        }
        String nodeAddress = UidMappingUtil.getInstance().hash(Long.valueOf(tmps[1]));
        //所有List<Put>
        List<Put> puts = pHBaseUps.get(nodeAddress);
        if (puts == null) {
          puts = new ArrayList<Put>();
          pHBaseUps.put(nodeAddress, puts);
        }

        Put put = new Put(shortenUid);

        for (Object entry : jsonMap.entrySet()) {
          if (entry instanceof Map.Entry) {
            String key = ((Map.Entry) entry).getKey().toString();
            String value = ((Map.Entry) entry).getValue().toString();

            int propertyID = ProjectPropertyCacheInHBase.getInstance().getPropertyID(tmps[0], key);
            if (propertyID > Constants.NULL_MAXPROPERTYID) {
              UpdateFunc upUpdateFunc = ProjectPropertyCacheInHBase.getInstance().getPropertyFunc(tmps[0], key);
              if (upUpdateFunc == UpdateFunc.once) {
                put.add(Bytes.toBytes(Constants.UP_COLUMNFAMILY), Bytes.toBytes(propertyID),
                        Helper.transformOnceTimestamp(), Bytes.toBytes(value));
                put.setDurability(Durability.SKIP_WAL);
              } else if (upUpdateFunc == UpdateFunc.cover) {
                put.add(Bytes.toBytes(Constants.UP_COLUMNFAMILY), Bytes.toBytes(propertyID),
                        Helper.getCurrentDayBeginTimestamp(), Bytes.toBytes(value));
                put.setDurability(Durability.SKIP_WAL);
              } else if (upUpdateFunc == UpdateFunc.inc) {
                put.add(Bytes.toBytes(Constants.UP_COLUMNFAMILY), Bytes.toBytes(propertyID),
                        System.currentTimeMillis(), Base64Util_Helper.toBytes(Long.parseLong(value)));
                put.setDurability(Durability.SKIP_WAL);
              }

            }
          }
        }
        if (!put.isEmpty()) {
          System.out.println(put.toJSON());
          puts.add(put);
        }


      } catch (IOException e) {
        LOG.warn("json parse error." + e.getMessage());
        LOG.warn(log);
      }

    }
    return hbaseUPs;
  }


}
