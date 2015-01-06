package com.xingcloud.server;

import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.mysql.UpdateFunc;
import org.apache.commons.dbcp.DelegatingStatement;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Author: liqiang
 * Date: 15-1-6
 * Time: 下午7:46
 */
public class LoadData {
    private static final Log LOG = LogFactory.getLog(LoadData.class);

    public static void main(String[] args){
        String[] projects = {"22apple","22find","aartemis","awesomehp","delta-homes","do-search","dosearches","istart123","istartsurf","key-find","lightning-newtab","lightning-speedi","mystartsearch","nationzoom","newtab2","omiga-plus","portaldosites","qone8","quick-start","qvo6","searchprotect","sof-dp","sof-dsk","sof-gdp","sof-hpprotect","sof-ient","sof-isafe","sof-mb","sof-newgdp","sof-nts","sof-windowspm","sof-wpm","sof-yacnvd","sof-zip","sweet-page","usv9","v9","vi-view","webssearches"};
        String[] attrs = {"ref0","register_time","geoip"};
        ExecutorService SERVICE = Executors.newFixedThreadPool(8);

        for(String p : projects){
            for(String t: attrs){
                SERVICE.submit(new Load(p,t));
            }
        }
        SERVICE.shutdown();
    }

    private static Connection getNodeConn(String project, String nodeAddress) throws SQLException {
        return MySql_16seqid.getInstance().getConnByNode(project, nodeAddress);
    }

    static class Load implements Runnable{

        String project;
        String tableName;

        public Load(String project, String tableName){
            this.project = project;
            this.tableName = tableName;
        }


        @Override
        public void run() {

            FileInputStream fis = null;
            BufferedReader reader = null;
            StringBuilder loadData = new StringBuilder("");
            System.out.println(project + ":" + tableName);
            try {
                fis = new FileInputStream("/data2/loadmysqltohdfs/" + project + "/" + tableName + ".txt");
                reader = new BufferedReader(new InputStreamReader(fis));
                String line;

                while((line =  reader.readLine()) != null){
                    loadData.append(line);
                }
            } catch (Exception e) {
            }
            System.out.println(project + ":" + tableName + " begin load");

            String loadDataSQL = null;

                loadDataSQL = "load data local infile 'ignore_me' " +
                        " replace into table " + tableName +
                        " character set utf8 " +
                        " fields terminated by '\t' optionally enclosed by '\"' escaped by '\"'";
//        loadDataSQL = String.format("LOAD DATA LOCAL INFILE 'ignore_me' REPLACE INTO TABLE %s;", tableName);

            if (loadDataSQL != null) {
                Connection loadDataConnection = null;
                com.mysql.jdbc.Statement loadDataStatement = null;

                long startTime = System.currentTimeMillis();



                    int tryTimes = 1;
                    boolean successful = false;

                    while (!successful) {
                        try {
                            // for each retry, initialize connection to null
                            loadDataConnection = null;
                            loadDataConnection = getNodeConn(project, "node6");
                            loadDataConnection.setAutoCommit(false);

                            Statement statement = loadDataConnection.createStatement();
                            statement = ((DelegatingStatement)statement).getInnermostDelegate();

                            assert statement != null && statement instanceof com.mysql.jdbc.Statement;

                            loadDataStatement = (com.mysql.jdbc.Statement)statement;
                            // by default, mysql jdbc driver sets sql_mode to STRICT_TRANS_TABLES
                            // by setting sql_mode to none, we disable data truncation exception
                            loadDataStatement.execute("set sql_mode=''");
                            loadDataStatement.setLocalInfileInputStream(IOUtils.toInputStream(loadData.toString(), Charsets.UTF_8));
                            loadDataStatement.execute(loadDataSQL);
                            loadDataConnection.commit();

                            successful = true;
                        } catch (SQLException sqle) {
                            LOG.error("load data failed. " + toString() +
                                    " retry load data in " + 10 * tryTimes / 1000 +
                                    " seconds." + sqle.getMessage());

                            if (loadDataConnection != null) {
                                try {
                                    loadDataConnection.rollback();
                                } catch (SQLException sqlexception) {
                                    LOG.error(sqlexception.getMessage());
                                }
                            }
                        } finally {
                            DbUtils.closeQuietly(loadDataStatement);
                            DbUtils.closeQuietly(loadDataConnection);
                        }

                        if (!successful) {
                            try {
                                Thread.sleep(10 * tryTimes);
                                tryTimes = (tryTimes << 1) & Integer.MAX_VALUE;
                                if(tryTimes > 1024){  //最多等待10多分钟
                                    tryTimes = 1024;
                                }
                            } catch (InterruptedException ie1) {
                                successful = true;
                            }
                        }
                    }

            }
        }
    }
}

