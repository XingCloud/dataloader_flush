package com.xingcloud.server.task;

import com.xingcloud.server.helper.Constants;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * User: IvyTang
 * Date: 13-8-5
 * Time: PM2:58
 */
public class EventFlushExecutor  extends ThreadPoolExecutor {


  public EventFlushExecutor() {
    super(Constants.HBASE_FLUSH_THREADS, Constants.HBASE_FLUSH_THREADS, 30, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
  }
}
