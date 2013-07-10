package com.xingcloud.server.helper;

/**
 * User: IvyTang
 * Date: 12-11-28
 * Time: 下午1:55
 */
public class Constants {

  public static final String EVENT = "event";

  public static final String USER = "user";

  public static final String TIMEZONE = "GMT+8";

  public static final int EVENT_ITEMS_NUM = 5;

  public static final int USER_ITEM_NUM = 3;

  public static final String UP_COLUMNFAMILY = "value";

  public static final String columnFamily = "val";

  public static boolean deuTableWalSwitch = false;

  public static String HBASE_PORT = "3181";

  public static int EXECUTOR_TIME_MIN = 15;

  public static int MYSQLBL_TIME_MIN = 12;

  public static int REDIS_UICHECK_NUM = 0;

  public static long HBASE_FLUSH_PERIOD = 3 * 3600 * 1000;

  public static long WRITE_BUFFER_SIZE = 1024 * 1024 * 20;

  public static boolean WRITE_SENDPROCESS_PER_BATCH = true;

  public static int EVENT_ONCE_READ = 6 * 10000;

  public static int HBASEUP_ONE_THREAD_PUT = 1;

  public static int HBASEUP_ONCE_READ = 10 * 10000;

  public static int USER_ONCE_READ = 3 * 10000;

  public static int USER_BULK_LOAD_ONCE_READ = 63 * 10000;

  public static int EXECUTOR_THREAD_COUNT = 20;

  public static int DELAY_ONCE_READ = 10000;

  public static final String EVENT_TAIL_CONF_PATH = "/data/log/eventfixconfig";

  public static final String HBASE_FLUSH_POINT = "hbase_flush_checkpoint";

  public static final String SEND_PROCESS = "sendlog.process";

  public static final String USER_TAIL_CONF_PATH = "/data/log/userfixconfig";

  public static final String HBASEUP_TAIL_CONF_PATH = "/data/log/hbaseupfixconfig";


  public static final String USER_LOAD_PATH = "/data/log/userload/";

  public static final int MYSQL_BATCH_UPDATE_SIZE = 1000;

  public static final String USER_DAYEND_LOG = "=======user day log end=======";

  public static final int SEND_FINISH_TRY_COUND = 100;

  public static final int USER_WAITOFFLINE_MIN = 120;

  public static final int USER_WAITOFFLINE_SLEEP_INTERVAL_MIN = 10;

  public static final int OFFLINE_DB = 15;

  public static final int NULL_MAXPROPERTYID = -1;

  public static final int MYSQL_BL_THREADS = 6;


}
