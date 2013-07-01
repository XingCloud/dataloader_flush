package com.xingcloud.server.task;

import com.xingcloud.server.helper.Constants;

import java.util.concurrent.*;

/**
 * User: IvyTang
 * Date: 12-11-29
 * Time: 下午12:02
 */
public class FlushExecutor extends ThreadPoolExecutor {


    public FlushExecutor() {
        super(Constants.EXECUTOR_THREAD_COUNT, Constants.EXECUTOR_THREAD_COUNT, 30, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>());
    }


}
