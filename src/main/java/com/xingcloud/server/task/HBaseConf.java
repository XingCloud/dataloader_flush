package com.xingcloud.server.task;

import com.xingcloud.server.helper.Constants;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * User: IvyTang
 * Date: 12-11-29
 * Time: 上午11:06
 */
public class HBaseConf {

    private static HBaseConf instance;

    private Map<String, Configuration> confs = new HashMap<String, Configuration>();

    private HBaseConf() {
        for (String hbaseAddress : UidMappingUtil.getInstance().nodes()) {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", hbaseAddress);
            conf.set("hbase.zookeeper.property.clientPort", Constants.HBASE_PORT);
            confs.put(hbaseAddress, conf);
        }
    }

    public static HBaseConf getInstance() {
        if (instance == null)
            instance = new HBaseConf();
        return instance;
    }

    public Configuration getHBaseConf(String hbaseAddress) {
        return confs.get(hbaseAddress);
    }
}
