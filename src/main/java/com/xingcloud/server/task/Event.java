package com.xingcloud.server.task;

import com.xingcloud.server.helper.Constants;
import com.xingcloud.server.helper.Helper;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.UnsupportedEncodingException;

/**
 * User: IvyTang
 * Date: 12-11-29
 * Time: 上午11:14
 */
public class Event {

    private byte[] rowKey;
    private long seqUid;
    private String eventStr;
    private long timeStamp;
    private long value;

    public Event(long seqUid, String eventStr, long timeStamp, long value) throws UnsupportedEncodingException {
        this.seqUid = seqUid;
        this.eventStr = eventStr;
        this.timeStamp = timeStamp;
        this.value = value;
        this.rowKey = constructRowKey(timeStamp, seqUid, eventStr);
    }

    private byte[] constructRowKey(long ts, long innerUid, String event) throws UnsupportedEncodingException {
        long samplingUid = UidMappingUtil.getInstance().decorateWithMD5(innerUid);
        return UidMappingUtil.getInstance().getRowKeyV2(Helper.getDate(ts),
                event, samplingUid);
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public long getSeqUid() {
        return seqUid;
    }

    public String getEventStr() {
        return eventStr;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public long getValue() {
        return value;
    }

    @Override
    public String toString() {
        return seqUid + "\t" + Bytes.toString(rowKey) + "\t" + eventStr + "\t" + timeStamp + "\t" + value;
    }
}
