package com.xingcloud.server.tailler;

import com.xingcloud.server.helper.Constants;
import com.xingcloud.server.helper.RedisResourceManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;

/**
 * User: IvyTang
 * Date: 13-2-27
 * Time: 下午2:43
 */
public class OfflineDumpMySQL {

    private static final Log LOG = LogFactory.getLog(OfflineDumpMySQL.class);

    public static boolean UserNeedCheckProcess = true;


    private static final String KEY_USERLOG = "userlog";

    private static final String KEY_MYSQLDUMP = "mysql_dump_status";

    public static void waitOfflineDumpMySQL() {

        UserNeedCheckProcess = false;
        Jedis jedis = null;
        //发送每天log flush结束信号到redis
        boolean sendFinish = false;
        for (int i = 0; i < Constants.SEND_FINISH_TRY_COUND; i++) {
            try {
                jedis = RedisResourceManager.getInstance().getCache(Constants.OFFLINE_DB);
                jedis.del(KEY_MYSQLDUMP);
                if (jedis.lpush(KEY_USERLOG, "all-finished") > 0) {
                    sendFinish = true;
                    break;
                }
            } catch (Exception e) {
                LOG.error("waitOfflineDumpMySQL send userlog all-finished error.", e);
                RedisResourceManager.getInstance().returnBrokenResource(jedis);
            } finally {
                RedisResourceManager.getInstance().returnResource(jedis);
            }
        }
        //发送成功后，
        if (sendFinish) {
            LOG.info("waitOfflineDumpMySQL send succeeds");
            for (int i = 0; i < Constants.USER_WAITOFFLINE_MIN / Constants.USER_WAITOFFLINE_SLEEP_INTERVAL_MIN; i++) {
                try {
                    jedis = RedisResourceManager.getInstance().getCache(Constants.OFFLINE_DB);
                    if (jedis.lpop(KEY_MYSQLDUMP) == null) {
                        Thread.sleep(Constants.USER_WAITOFFLINE_SLEEP_INTERVAL_MIN * 60 * 1000);
                        LOG.info("waitOfflineDumpMySQL waiting mysql dump completing...");
                    } else {
                        LOG.info("waitOfflineDumpMySQL mysql dump completed.");
                        break;
                    }
                } catch (Throwable e) {
                    LOG.error("waitOfflineDumpMySQL send userlog all-finished error.", e);
                    RedisResourceManager.getInstance().returnBrokenResource(jedis);
                } finally {
                    RedisResourceManager.getInstance().returnResource(jedis);
                }
            }
        } else {
            LOG.error("waitOfflineDumpMySQL send user log not succeeds.");
        }


        LOG.info("waitOfflineDumpMySQL begin clear.");
        //clear redis ,delete userlog
        try {
            jedis = RedisResourceManager.getInstance().getCache(Constants.OFFLINE_DB);
            jedis.del(KEY_USERLOG);
            LOG.info("waitOfflineDumpMySQL clear redis.");
        } catch (Throwable e) {
            LOG.error("waitOfflineDumpMySQL delete userlog  error.", e);
            RedisResourceManager.getInstance().returnBrokenResource(jedis);
        } finally {
            RedisResourceManager.getInstance().returnResource(jedis);
        }

        UserNeedCheckProcess = true;
    }
}
