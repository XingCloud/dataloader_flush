package com.xingcloud.server.task;

import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

/**
 * User: IvyTang
 * Date: 12-12-3
 * Time: 下午2:28
 */
public class HBaseFlushTask implements Runnable {

    private static final Log LOG = LogFactory.getLog(HBaseFlushTask.class);

    private String project;

    public HBaseFlushTask(String project) {
        this.project = project;
    }

    @Override
    public void run() {
        for (String hbaseNode : UidMappingUtil.getInstance().nodes()) {
            HBaseAdmin hBaseAdmin = null;
            Configuration conf = null;
            try {
                conf = HBaseFlushConf.getInstance().getHBaseConf(hbaseNode);
                hBaseAdmin = new HBaseAdmin(conf);
                long flushBeginTime = System.currentTimeMillis();
                hBaseAdmin.flush("deu_" + project);
                LOG.info("hbase flush " + project + " " + conf.get("hbase.zookeeper.quorum") + "using"
                        + (System.currentTimeMillis() - flushBeginTime) + "ms");
            } catch (Exception e) {
                LOG.error("hbase flush " + project + " error." + conf.get("hbase.zookeeper.quorum") + e.getMessage());
            } finally {
                try {
                    if (hBaseAdmin != null)
                        hBaseAdmin.close();
                } catch (IOException e) {
                    LOG.error(project + e.getMessage(), e);
                }
            }
        }
    }
}
