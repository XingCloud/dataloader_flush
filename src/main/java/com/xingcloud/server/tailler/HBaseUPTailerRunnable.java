package com.xingcloud.server.tailler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * User: IvyTang
 * Date: 13-7-8
 * Time: PM3:28
 */
public class HBaseUPTailerRunnable  implements Runnable{

  private static final Log LOG = LogFactory.getLog(HBaseUPTailerRunnable.class);

  private  HBaseUPTailer hBaseUPTailer;

  public HBaseUPTailerRunnable(HBaseUPTailer hBaseUPTailer) {
    this.hBaseUPTailer = hBaseUPTailer;
  }

  @Override
  public void run() {
    try {
      hBaseUPTailer.start();
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }
}
