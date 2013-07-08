package com.xingcloud.server.tailler;

import com.xingcloud.server.helper.Constants;
import com.xingcloud.server.helper.Helper;
import com.xingcloud.server.helper.ProjectPropertyCacheInHBase;
import com.xingcloud.server.task.*;
import com.xingcloud.userprops_meta_util.UpdateFunc;
import com.xingcloud.xa.uidmapping.UidMappingUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
 * Date: 13-5-13
 * Time: 上午11:57
 */
public class UserTailer_BulkLoad extends Tail {
  private static final Log LOG = LogFactory.getLog(UserTailer_BulkLoad.class);


  public UserTailer_BulkLoad(String configPath) {
    super(configPath);
    setBatchSize(Constants.USER_BULK_LOAD_ONCE_READ);
    setLogProcessPerBatch(Constants.WRITE_SENDPROCESS_PER_BATCH);
    LOG.info(configPath);
    LOG.info(this.datafile);
    LOG.info(this.day);
  }

  @Override
  public void send(List<String> strings, long l) {
    LOG.info("======UserTailer_BulkLoad======= " + l + " users log ..." + strings.size());
    long currentTime = System.currentTimeMillis();
    Map<String, List<User_BulkLoad>> usersMap = new HashMap<String, List<User_BulkLoad>>();
    Map<String, Map<String, Map<String, List<Put>>>> traceableProperties = new HashMap<String,
            Map<String, Map<String, List<Put>>>>();
    analysisUser(strings, usersMap, traceableProperties);

    for (Map.Entry<String, Map<String, Map<String, List<Put>>>> entry : traceableProperties.entrySet()) {
      System.out.println(entry.getKey());
      for (Map.Entry<String, Map<String, List<Put>>> nodeEntry : entry.getValue().entrySet()) {
        System.out.println("\t" + nodeEntry.getKey());
        for (Map.Entry<String, List<Put>> pEntry : nodeEntry.getValue().entrySet()) {
          System.out.println("\t\t" + pEntry.getKey());
          for (Put put : pEntry.getValue()) {
            byte[] inneruid = {put.getRow()[1], put.getRow()[2], put.getRow()[3], put.getRow()[4]};
            System.out.println("\t\t\t" + Bytes.toInt(inneruid) + "\t" + UidMappingUtil.getInstance().hash(Bytes.toInt
                    (inneruid)) + "\t" + Bytes.toString(put.get(Bytes.toBytes(Constants.UP_COLUMNFAMILY),
                    Bytes.toBytes(Constants.UP_COLUMNFAMILY)).get(0).getValue()));
          }
        }
      }
    }
    try {
      FlushExecutor userExecutor = new FlushExecutor();

      //random pids sort, let 4 dataloader load data in different pids sort .
//      String[] randomPids = usersMap.keySet().toArray(new String[usersMap.keySet().size()]);
//      Helper.shuffle(randomPids);
//      for (String pid : randomPids) {
//        USerTask_BulkLoad uSerTask_bulkLoad = new USerTask_BulkLoad(pid, usersMap.get(pid));
//        userExecutor.execute(uSerTask_bulkLoad);
//      }


      userExecutor.execute(new TraceablePropertiesTask(traceableProperties));
      userExecutor.shutdown();
      boolean result = userExecutor.awaitTermination(Constants.EXECUTOR_TIME_MIN, TimeUnit.MINUTES);
      if (!result) {
        LOG.warn("userExecutor_bulkload timeout....throws this exception to tailer and quit this.");
        userExecutor.shutdownNow();
        throw new RuntimeException("userExecutor timeout.");
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
    LOG.info("======UserTailer_BulkLoad======= " + l + " users log send completed." + strings.size()
            + "  using " + (System.currentTimeMillis() - currentTime) + "ms. pids:" + usersMap.keySet());
  }

  private void analysisUser(List<String> logs, Map<String, List<User_BulkLoad>> usersMap,
                            Map<String, Map<String, Map<String, List<Put>>>> traceableProperties) {//
    // traceableProperties的结构为<nodeAddress,<project,<tablename,List<Put>>>
    ObjectMapper objectMapper = new ObjectMapper();
    for (String log : logs) {
      String[] tmps = log.split("\t");
      if (tmps.length != Constants.USER_ITEM_NUM) {
        LOG.warn(log);
        continue;
      }
      long samplingUid = UidMappingUtil.getInstance().decorateWithMD5(Long.valueOf(tmps[1]));
      List<String> propKeys = new ArrayList<String>();
      List<String> propValues = new ArrayList<String>();
      try {
        Map jsonMap = objectMapper.readValue(tmps[2], Map.class);
        for (Object entry : jsonMap.entrySet()) {
          if (entry instanceof Map.Entry) {
            String key = ((Map.Entry) entry).getKey().toString();
            String value = ((Map.Entry) entry).getValue().toString();
            propKeys.add(key);
            propValues.add(value);
            if (ProjectPropertyCacheInHBase.getInstance().getPropertyID(tmps[0], key) > Constants.NULL_MAXPROPERTYID) {

              String nodeAddress = UidMappingUtil.getInstance().hash(Long.valueOf(tmps[1]));

              // 拿到一个hbase node对应的所有put
              Map<String, Map<String, List<Put>>> nodePropertiesPuts = traceableProperties.get(nodeAddress);
              if (nodePropertiesPuts == null) {
                nodePropertiesPuts = new HashMap<String, Map<String, List<Put>>>();
                traceableProperties.put(nodeAddress, nodePropertiesPuts);
              }

              //一个hbase node上一个项目的所有put
              Map<String, List<Put>> pPuts = nodePropertiesPuts.get(tmps[0]);
              if (pPuts == null) {
                pPuts = new HashMap<String, List<Put>>();
                nodePropertiesPuts.put(tmps[0], pPuts);
              }

              //一个hbase node上一个项目的一个traceable属性的所有的put
              List<Put> puts = pPuts.get(key);
              if (puts == null) {
                puts = new ArrayList<Put>();
                pPuts.put(key, puts);
              }

              byte[] uidBytes = Bytes.toBytes(samplingUid);
              byte[] shortenUid = {uidBytes[3], uidBytes[4], uidBytes[5], uidBytes[6], uidBytes[7]};
              Put put = new Put(shortenUid);

              if (ProjectPropertyCacheInHBase.getInstance().getPropertyFunc(tmps[0], key) == UpdateFunc.once)
                put.add(Bytes.toBytes(Constants.UP_COLUMNFAMILY), Bytes.toBytes(Constants.UP_COLUMNFAMILY),
                        Helper.transformOnceTimestamp(), Bytes.toBytes(value));
              else
                put.add(Bytes.toBytes(Constants.UP_COLUMNFAMILY), Bytes.toBytes(Constants.UP_COLUMNFAMILY),
                        Helper.getCurrentDayBeginTimestamp(), Bytes.toBytes(value));
              puts.add(put);
            }
          }
        }
      } catch (IOException e) {
        LOG.warn("json parse error." + e.getMessage());
        LOG.warn(log);
        continue;
      }
      User_BulkLoad user = new User_BulkLoad(tmps[0], Long.valueOf(tmps[1]), samplingUid, propKeys, propValues);
      List<User_BulkLoad> users = usersMap.get(tmps[0]);
      if (users == null) {
        users = new ArrayList<User_BulkLoad>();
        usersMap.put(tmps[0], users);
      }
      users.add(user);
    }

  }
}
