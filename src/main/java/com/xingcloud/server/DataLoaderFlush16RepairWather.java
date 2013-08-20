package com.xingcloud.server;


import com.xingcloud.server.exception.ConfException;
import com.xingcloud.server.hbaseflush.HBaseFlushRunnable;
import com.xingcloud.server.helper.Constants;

import com.xingcloud.server.helper.Helper;
import com.xingcloud.server.helper.Log4jProperties;
import com.xingcloud.server.tailler.*;
import com.xingcloud.server.task.MonitorRunnable;
import com.xingcloud.server.task.USerTask_BulkLoad;
import com.xingcloud.xa.hash.HashUtil;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * User: IvyTang
 * Date: 12-11-28
 * Time: 下午1:14
 */
public class DataLoaderFlush16RepairWather {

  private static final Log LOG = LogFactory.getLog(DataLoaderFlush16RepairWather.class);

  public static void main(String[] args) throws IOException, ConfException {

    Log4jProperties.init();
    LOG.info(UidMappingUtil.getInstance().nodes());

    Thread eventThread = new Thread(new EventTailerRunnable(new EventTailer(Constants.EVENT_TAIL_CONF_PATH)));
    eventThread.start();
    LOG.info("event thread starting...");


  }
}
