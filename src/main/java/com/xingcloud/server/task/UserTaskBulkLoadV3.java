package com.xingcloud.server.task;

import com.xingcloud.mysql.MySqlDict;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.mysql.UpdateFunc;
import com.xingcloud.mysql.UserProp;
import com.xingcloud.server.helper.Constants;
import com.xingcloud.server.helper.Helper;
import com.xingcloud.server.helper.Pair;
import com.xingcloud.server.helper.ProjectPropertyCache;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.dbcp.DelegatingStatement;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * User: 李强
 * Date: 15-1-1
 * Time: 下午18:33
 */
public class UserTaskBulkLoadV3 implements Runnable {

    private static final Log LOG = LogFactory.getLog(UserTaskBulkLoadV3.class);
    private String project;
    private List<User_BulkLoad> users;
    private ProjectPropertyCache projectPropertyCache;

    public UserTaskBulkLoadV3(String project, List<User_BulkLoad> users) {
        this.project = project;
        this.users = users;
        // todo: load user properties from mysql each time?
        this.projectPropertyCache = ProjectPropertyCache.resetProjectCache(project);
    }


    @Override
    public void run() {
        try {
            LOG.info("enter user task. user size:" + users.size() + "\tproject: " + project);

            // node, rowkey, value
            Map<String, Map<UpdateFunc, Map<byte[], Object>>> nodeTableSBMap = new HashMap<String, Map<UpdateFunc, Map<byte[], Object>>>();

            long startTime = System.currentTimeMillis();
            prepareUsers(nodeTableSBMap);

            LOG.info("USerTask_BulkLoad==== " + project + " prepareUsers using time: "
                    + (System.currentTimeMillis() - startTime) + "ms. size:" + users.size());

            startTime = System.currentTimeMillis();
            bulkLoad(nodeTableSBMap);

            LOG.info("USerTask_BulkLoad==== " + project + " bulkLoad using time: "
                    + (System.currentTimeMillis() - startTime) + "ms. size:" + users.size());

        } catch (Exception e) {
            LOG.error("end this thread.", e);
        }
    }


    private byte[] createRowKey(String pid, String attr, long uid) throws Exception {
        int pidDict = Constants.dict.getPidDict(pid);
        int attrDict = Constants.dict.getAttributeDict(attr);
        return Bytes.add(Bytes.toBytes(pidDict), Bytes.toBytes(attrDict), Bytes.toBytes(uid));
    }

    /**
     * 分析user.log，生成每个node对应的rowkey和value
     *
     * @param nodeTableSBMap
     */
    private void prepareUsers(Map<String, Map<UpdateFunc, Map<byte[], Object>>> nodeTableSBMap) throws Exception {
        // sort List<User_BulkLoad> users, in user hash uid order.
        Collections.sort(users, new UserComparator());

        for (User_BulkLoad user : users) {
            String nodeAddress = UidMappingUtil.getInstance().hash(user.getSeqUid());

            List<String> propKeys = user.getPropKeys();
            List<String> propValues = user.getPropValues();

            for (int i = 0; i < propKeys.size(); i++) {
                String key = propKeys.get(i); // table name
                String value = propValues.get(i); // row value

                UserProp userProp = projectPropertyCache.getUserPro(key);
                if (userProp == null) {
                    LOG.error("user property is null for key: " + key + ", project: " + project);
                    continue;
                }

                Map<UpdateFunc, Map<byte[], Object>> lists = nodeTableSBMap.get(nodeAddress);
                if (lists == null) {
                    lists = new HashMap<UpdateFunc, Map<byte[], Object>>();
                    lists.put(UpdateFunc.inc, new HashMap<byte[], Object>());
                    lists.put(UpdateFunc.cover, new HashMap<byte[], Object>());
                    lists.put(UpdateFunc.once, new HashMap<byte[], Object>());
                }

                Object result = value;
                switch (userProp.getPropType()) {
                    case sql_bigint:
                        result = Long.parseLong(value);
                        break;
                    case sql_datetime:
                        result = Long.parseLong(value);
                        break;
                    case sql_string:

                }

                lists.get(userProp.getPropFunc()).put(createRowKey(project, key, user.getSamplingUid()), result);
            }
        }
    }


    private void bulkLoad(Map<String, Map<UpdateFunc, Map<byte[], Object>>> nodeTableSBMap) throws InterruptedException {

        List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
        for (Map.Entry<String, Map<UpdateFunc, Map<byte[], Object>>> entry : nodeTableSBMap.entrySet()) {
            UserChildThread userChildThread = new UserChildThread(entry.getKey(), project, entry.getValue());
            futures.add(MySQLBulkLoadExecutor.getInstance().submit(userChildThread));
        }

        for (Future<Boolean> booleanFuture : futures) {
            try {
                booleanFuture.get();
            } catch (ExecutionException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

}

class UserChildThread implements Callable<Boolean> {

    private static final Log LOG = LogFactory.getLog(UserChildThread.class);

    private String node;
    private String project;
    private Map<UpdateFunc, Map<byte[], Object>> attrs;

    public UserChildThread(String node, String project, Map<UpdateFunc, Map<byte[], Object>> attrs) {
        this.node = node;
        this.project = project;
        this.attrs = attrs;
    }

    public Boolean call() {
        int count = 0;
        for (Map.Entry<UpdateFunc, Map<byte[], Object>> users : attrs.entrySet()) {
            count += users.getValue().size();
            while (true) {
                HTable table = null;
                long currentTime = System.currentTimeMillis();
                try {

                    List<Put> puts = new ArrayList<Put>();
                    for (Map.Entry<byte[], Object> user : users.getValue().entrySet()) {
                        Put put = new Put(user.getKey());
                        put.setWriteToWAL(Constants.deuTableWalSwitch);
                        byte[] value = null;
                        if(user.getValue() instanceof Long){
                            value = Bytes.toBytes((Long)user.getValue());
                        }else{
                            value = Bytes.toBytes((String)user.getValue());
                        }
                        put.add(Constants.userColumnFamily.getBytes(), Constants.userColumnFamily.getBytes(),value);
                        puts.add(put);
                    }

                    // todo: use connection pool?
                    table = new HTable(HBaseConf.getInstance().getHBaseConf(node), "user_attribute");
                    table.setAutoFlush(false);
                    table.setWriteBufferSize(Constants.WRITE_BUFFER_SIZE);

                    if (users.getKey() == UpdateFunc.cover) {
                        table.put(puts);
                    } else if (users.getKey() == UpdateFunc.once) {
                        for (Put put : puts) {
                            table.checkAndPut(put.getRow(), Constants.userColumnFamily.getBytes(), Constants.userColumnFamily.getBytes(), null, put);
                        }
                    } else if (users.getKey() == UpdateFunc.inc) {
                        for (Map.Entry<byte[], Object> user : users.getValue().entrySet()) {
                            table.incrementColumnValue(user.getKey(), Constants.userColumnFamily.getBytes(), Constants.userColumnFamily.getBytes(), (Long)user.getValue(), false);
                        }
                    }

                    table.flushCommits();
                    LOG.info(project + " " + node + " " + users.getKey().name() + " put hbase size:" + users.getValue().size() +
                            " completed .tablename is " + Helper.getHBaseTableName(project) + " using "
                            + (System.currentTimeMillis() - currentTime) + "ms");
                    break;
                } catch (Exception e) {
                    if (e.getMessage().contains("interrupted")) {
                        break;
                    }
                    LOG.error(project + node + e.getMessage(), e);
                    if (e.getMessage().contains("HConnectionImplementation") && e.getMessage().contains("closed")) {
                        HConnectionManager.deleteConnection(HBaseConf.getInstance().getHBaseConf(node), true);
                        LOG.warn("delete connection to " + node);
                    }

                    LOG.info("trying put hbase " + project + " " + node + "again...tablename " +
                            ":" + Helper.getHBaseTableName(project));
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        break;
                    }
                } finally {
                    try {
                        if (table != null) {
                            table.close();
                        }
                    } catch (IOException e) {
                        LOG.error(project + e.getMessage(), e);
                    }
                }
            }
        }
        LOG.info("enter run user task. " + project + " user size:" + count);
        return true;
    }

}

