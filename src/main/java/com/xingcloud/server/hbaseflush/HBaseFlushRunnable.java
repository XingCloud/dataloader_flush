package com.xingcloud.server.hbaseflush;

import com.xingcloud.server.helper.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * User: IvyTang
 * Date: 12-12-17
 * Time: 下午2:09
 */
public class HBaseFlushRunnable implements Runnable {

    private static final Log LOG = LogFactory.getLog(HBaseFlushRunnable.class);

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(Constants.HBASE_FLUSH_PERIOD);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage());
            }
            HBaseFlushStatus.FLUSHSTATUS = HBaseFlushStatus.NEEDFLUSH;
            LOG.info("need hbase flush...sending this signal..");
        }
    }

}