package com.xingcloud.server.tailler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * User: IvyTang
 * Date: 12-11-29
 * Time: 下午2:51
 */
public class EventTailerRunnable implements Runnable {

    private static final Log LOG = LogFactory.getLog(EventTailerRunnable.class);

    private EventTailer eventTailer;

    public EventTailerRunnable(EventTailer eventTailer) {
        this.eventTailer = eventTailer;
    }

    @Override
    public void run() {
        try {
            eventTailer.start();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }
}
