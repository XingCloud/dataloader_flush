package com.xingcloud.server.helper;

import com.xingcloud.server.task.HBaseConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import java.io.IOException;


/**
 * Created by Administrator on 2015/1/3.
 */
public class HBaseTool {

    public static void main(String[] args) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor("user_attribute");

        HColumnDescriptor column = new HColumnDescriptor("v");
        column.setCompressionType(Compression.Algorithm.LZO);
        column.setMaxVersions(1);
        column.setBloomFilterType(StoreFile.BloomType.ROWCOL);
        tableDescriptor.addFamily(column);

        for(int i=0;i<=15;i++){
            String node = "node" + i;
            System.out.println(node);
            Configuration conf = HBaseConf.getInstance().getHBaseConf(node);
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.createTable(tableDescriptor);
        }
    }
}
