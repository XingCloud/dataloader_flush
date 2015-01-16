package com.xingcloud.server;

import com.xingcloud.server.exception.ConfException;
import com.xingcloud.server.helper.Constants;
import com.xingcloud.server.helper.Log4jProperties;
import com.xingcloud.server.tailler.*;
import com.xingcloud.xa.uidmapping.UidMappingUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * User: IvyTang
 * Date: 12-11-28
 * Time: 下午1:14
 */
public class DataLoaderFlush16TmpWather {

  private static final Log LOG = LogFactory.getLog(DataLoaderFlush16TmpWather.class);

  public static void main(String[] args) throws IOException, ConfException {
    Log4jProperties.init();
    LOG.info(UidMappingUtil.getInstance().nodes());

    Thread eventThread = new Thread(new EventTailerRunnable(new EventTailer(Constants.EVENT_TAIL_CONF_PATH)));
//    Thread userBulkLoadThread = new Thread(
//      new UserTailerRunnable_BulkLoad(new UserTailer_BulkLoad(Constants.USER_TAIL_CONF_PATH)));

    eventThread.start();
    LOG.info("event thread starting...");

//    userBulkLoadThread.start();
    LOG.info("user bulkload thread starting...");
  }
}

