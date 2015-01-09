package com.xingcloud.server.tailler;

import com.xingcloud.server.helper.Constants;
import com.xingcloud.server.helper.Helper;
import com.xingcloud.server.task.*;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;
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
    Map<String, List<User_BulkLoad>> usersMap = analysisUser(strings);
    try {
      FlushExecutor userExecutor = new FlushExecutor();
//      for (Map.Entry<String, List<User_BulkLoad>> entry : usersMap.entrySet()) {
//        USerTask_BulkLoad uSerTask_bulkLoad = new USerTask_BulkLoad(
//                entry.getKey(), entry.getValue());
//        userExecutor.execute(uSerTask_bulkLoad);
//      }

      //random pids sort, let 4 dataloader load data in different pids sort .
//      String[] randomPids = usersMap.keySet().toArray(new String[usersMap.keySet().size()]);

        //数据量大的项目入库时间长，将量大的前16个项目单独shuffle，放到最前面，减少入库时间
        usersMap.remove("newtabv1-bg");
        usersMap.remove("newtabv3-bg");
        List<Map.Entry<String, List<User_BulkLoad>>> entryList = new ArrayList<Map.Entry<String, List<User_BulkLoad>>>(usersMap.entrySet());
        Collections.sort(entryList, new Comparator<Map.Entry<String, List<User_BulkLoad>>>() {
            @Override
            public int compare(Map.Entry<String, List<User_BulkLoad>> o1, Map.Entry<String, List<User_BulkLoad>> o2) {
                if (o1.getValue().size() > o2.getValue().size()) {
                    return -1;
                }
                return o1.getValue().size() == o2.getValue().size() ? 0 : 1;
            }
        });

        int headCount = 40;

        int headLength = entryList.size() > headCount ? headCount: entryList.size();
        int tailLength = entryList.size() > headCount ? entryList.size() - headCount : 0;
        String[] headPids = new String[headLength];
        String[] tailPids = new String[tailLength];
        for(int i =0 ; i< entryList.size(); i++){
            if(i >= headCount){
                tailPids[i - headCount] = entryList.get(i).getKey();
            }else{
                headPids[i] = entryList.get(i).getKey();
            }
        }

        Helper.shuffle(headPids);
        for (String pid : headPids) {
            UserTaskBulkLoadV2 userTaskBulkLoadV2 = new UserTaskBulkLoadV2(pid, usersMap.get(pid));
            userExecutor.execute(userTaskBulkLoadV2);
        }

        Helper.shuffle(tailPids);
        for (String pid : tailPids) {
            UserTaskBulkLoadV2 userTaskBulkLoadV2 = new UserTaskBulkLoadV2(pid, usersMap.get(pid));
            userExecutor.execute(userTaskBulkLoadV2);
        }

/*      Helper.shuffle(randomPids);
      for (String pid : randomPids) {
//        USerTask_BulkLoad uSerTask_bulkLoad = new USerTask_BulkLoad(pid, usersMap.get(pid));
//        userExecutor.execute(uSerTask_bulkLoad);
        UserTaskBulkLoadV2 userTaskBulkLoadV2 = new UserTaskBulkLoadV2(pid, usersMap.get(pid));
        userExecutor.execute(userTaskBulkLoadV2);
      }*/

      userExecutor.shutdown();
      boolean result = userExecutor.awaitTermination(Constants.EXECUTOR_TIME_MIN, TimeUnit.MINUTES);
      if (!result) {
        LOG.warn("userExecutor_bulkload timeout....throws this exception to tailer and quit this.");
        userExecutor.shutdownNow();
        throw new RuntimeException("userExecutor timeout.");
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(),e);
    }

    LOG.info("======UserTailer_BulkLoad======= " + l + " users log send completed." + strings.size()
            + "  using " + (System.currentTimeMillis() - currentTime) + "ms. pids:" + usersMap.keySet());
  }

  private Map<String, List<User_BulkLoad>> analysisUser(List<String> logs) {
    Map<String, List<User_BulkLoad>> usersMap = new HashMap<String, List<User_BulkLoad>>();
    ObjectMapper objectMapper = new ObjectMapper();
    for (String log : logs) {
      String[] tmps = log.split("\t");
      if (!(tmps.length == Constants.USER_ITEM_NUM || tmps.length == Constants.USER_ITEM_NUM+1)) {
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
            if (((Map.Entry) entry).getKey() != null && ((Map.Entry) entry).getValue() != null) {
              propKeys.add(((Map.Entry) entry).getKey().toString());
              propValues.add(((Map.Entry) entry).getValue().toString());
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
    return usersMap;
  }
}
