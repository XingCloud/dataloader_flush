package com.xingcloud.server.task;

import com.xingcloud.server.helper.Constants;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * User: IvyTang
 * Date: 13-5-27
 * Time: 下午5:25
 */
public class MySQLBulkLoadExecutor extends ThreadPoolExecutor {

  private static final MySQLBulkLoadExecutor instance = new MySQLBulkLoadExecutor();


  private MySQLBulkLoadExecutor() {
    super(Constants.MYSQL_BL_THREADS, Constants.MYSQL_BL_THREADS, 30, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
  }

  public static MySQLBulkLoadExecutor getInstance() {
    return instance;
  }
}

