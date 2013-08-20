package com.xingcloud.server.tailler;

import com.xingcloud.server.hbaseflush.HBaseFlushStatus;
import com.xingcloud.server.helper.Constants;
import com.xingcloud.server.helper.Helper;
import com.xingcloud.server.helper.SpecialProjects;
import com.xingcloud.server.task.Event;
import com.xingcloud.server.task.EventTask;
import com.xingcloud.server.task.FlushExecutor;
import com.xingcloud.server.task.HBaseFlushTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * User: IvyTang
 * Date: 12-11-29
 * Time: 下午2:10
 */
public class EventTailer extends Tail {

    private static final Log LOG = LogFactory.getLog(EventTailer.class);


    public EventTailer(String configPath) {
        super(configPath);
        setBatchSize(Constants.EVENT_ONCE_READ);
        setLogProcessPerBatch(Constants.WRITE_SENDPROCESS_PER_BATCH);
        LOG.info(configPath);
        LOG.info(this.datafile);
        LOG.info(this.day);
    }

    @Override
    public void send(List<String> strings, long l) {
        LOG.info("======EventTailer=======" + l + " events log ..." + strings.size());
        long currentTime = System.currentTimeMillis();
        try {
            FlushExecutor eventExecutor = new FlushExecutor();
            Map<String, List<Event>> putsMap = analysisyEvent(strings);
            for (Map.Entry<String, List<Event>> entry : putsMap.entrySet()) {
                EventTask eventTask = new EventTask(entry.getKey(), entry.getValue());
                eventExecutor.execute(eventTask);
            }
            eventExecutor.shutdown();
            boolean result = eventExecutor.awaitTermination(Constants.EXECUTOR_TIME_MIN, TimeUnit.MINUTES);
            if (!result) {
                LOG.warn("eventExecutor timeout....throws this exception to tailer and quit this.");
                eventExecutor.shutdownNow();
                throw new RuntimeException("eventExecutor timeout.");
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
        LOG.info("======EventTailer=======" +l + " events log send completed." + strings.size() +
                " using " + (System.currentTimeMillis() - currentTime) + "ms.");
    }

    private Map<String, List<Event>> analysisyEvent(List<String> logs) {
        Map<String, List<Event>> putsMap = new HashMap<String, List<Event>>();
        for (String log : logs) {
            String[] tmps = log.split("\t");
            if (tmps.length != Constants.EVENT_ITEMS_NUM) {
                LOG.warn(log);
                continue;
            }
            if(!SpecialProjects.getSpecialProjects().contains(tmps[0]))
                continue;
            Event event = null;
            try {
                event = new Event(Long.valueOf(tmps[1]), tmps[2], Long.valueOf(tmps[4]), Long.valueOf(tmps[3]));
                List<Event> events = putsMap.get(tmps[0]);
                if (events == null) {
                    events = new ArrayList<Event>();
                    putsMap.put(tmps[0], events);
                }
                events.add(event);
            } catch (UnsupportedEncodingException e) {
                continue;
            }

        }
        return putsMap;
    }
}
