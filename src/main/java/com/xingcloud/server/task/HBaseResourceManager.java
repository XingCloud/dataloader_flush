package com.xingcloud.server.task;

import com.xingcloud.server.helper.Constants;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: liqiang
 * Date: 15-1-7
 * Time: 下午2:38
 */
public class HBaseResourceManager {

    private static Map<String, HTablePool> pools = new HashMap<String, HTablePool>();

    private static HBaseResourceManager m_instance;

    public synchronized static HBaseResourceManager getInstance() throws IOException {
        if (m_instance == null) {
            m_instance = new HBaseResourceManager();
        }
        return m_instance;
    }


    private HBaseResourceManager() throws IOException {
        for (String hbaseAddress : UidMappingUtil.getInstance().nodes()) {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", hbaseAddress);
            conf.set("hbase.zookeeper.property.clientPort", Constants.HBASE_PORT);
            HTablePool pool = new HTablePool(conf, 100);
            pools.put(hbaseAddress, pool);
        }
    }

    public HTableInterface getTable(String hbaseAddress, String tableName){
        return  pools.get(hbaseAddress).getTable(tableName);
    }
}
