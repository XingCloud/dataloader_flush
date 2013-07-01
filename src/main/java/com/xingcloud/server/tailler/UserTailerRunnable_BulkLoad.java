package com.xingcloud.server.tailler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * User: IvyTang
 * Date: 13-5-14
 * Time: 上午11:04
 */
public class UserTailerRunnable_BulkLoad implements Runnable {
  private static final Log LOG = LogFactory.getLog(UserTailerRunnable_BulkLoad.class);

  private UserTailer_BulkLoad userTailer;

  public UserTailerRunnable_BulkLoad(UserTailer_BulkLoad userTailer) {
    this.userTailer = userTailer;
  }


  @Override
  public void run() {
    try {
      userTailer.start();
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }

  }
}
