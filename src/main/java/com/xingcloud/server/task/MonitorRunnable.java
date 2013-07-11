package com.xingcloud.server.task;

import com.xingcloud.server.helper.Constants;
import com.xingcloud.server.tailler.OfflineDumpMySQL;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * User: IvyTang
 * Date: 13-3-27
 * Time: 上午10:38
 */
public class MonitorRunnable implements Runnable {

  private static final Log LOG = LogFactory.getLog(MonitorRunnable.class);

  private static final int FIRST_SLEEP_MS = 20 * 60 * 1000;

  private static final int CHECK_INTERVAL_MS = 10 * 60 * 1000;

  private long lastEventSendProcess = 0l;

  private long lastUserSendProcess = 0l;

  private int logTimes = 5;

  @Override
  public void run() {
    try {
      Thread.sleep(FIRST_SLEEP_MS);
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
    }
    while (true) {
      //check event
      String eventCmd = " tail -n 1 " + Constants.EVENT_TAIL_CONF_PATH + "/sendlog.process |awk '{print $2}'";
      long nowEventProcess = getLastSendProcess(eventCmd);
      if (nowEventProcess == lastEventSendProcess)
        for (int i = 0; i < logTimes; i++)
          LOG.error("event tailer error.process is " + nowEventProcess);
      else
        LOG.info("event tailer sending continuously:lastEventSendProcess" + lastEventSendProcess + ",nowEventProcess:" + nowEventProcess);
      lastEventSendProcess = nowEventProcess;

      //check user
      if (OfflineDumpMySQL.UserNeedCheckProcess) {
        String userCmd = " tail -n 1 " + Constants.USER_TAIL_CONF_PATH + "/sendlog.process |awk '{print $2}'";
        long nowUserProcess = getLastSendProcess(userCmd);
        if (nowUserProcess == lastUserSendProcess)
          for (int i = 0; i < logTimes; i++)
            LOG.error("user tailer error.process is " + nowUserProcess);
        else
          LOG.info("user tailer sending continuously:lastUserSendProcess" + lastUserSendProcess + "," +
                  "nowUserProcess:" + nowUserProcess);
        lastUserSendProcess = nowUserProcess;
      }

      try {
        Thread.sleep(CHECK_INTERVAL_MS);
      } catch (InterruptedException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }

  }


  private long getLastSendProcess(String cmd) {
    try {
      Runtime rt = Runtime.getRuntime();
      String[] cmds = new String[]{"/bin/sh", "-c", cmd};
      Process process = rt.exec(cmds);
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String cmdOutput = bufferedReader.readLine();
      if (cmdOutput == null)
        LOG.error(cmd + " error.");
      else
        return Long.parseLong(cmdOutput);
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
    return 0;
  }


}
