package com.xingcloud.server.helper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * User: IvyTang
 * Date: 13-2-27
 * Time: 上午11:42
 */
public class RedisShardedPoolResourceManager {
    private static Log logger = LogFactory.getLog(RedisShardedPoolResourceManager.class);

    private static ShardedJedisPool pool;

    private RedisShardedPoolResourceManager() {
        init();
    }

    private void init() {
        if (pool == null) {
            logger.info("First init Redis sharded pool...");
            List<JedisShardInfo> shardList = new ArrayList<JedisShardInfo>();

            int maxActive = Integer.parseInt(ConfigReader.getConfig("Redis.xml", "redis_sharded", "common", "max_active"));
            int maxIdle = Integer.parseInt(ConfigReader.getConfig("Redis.xml", "redis_sharded", "common", "max_idle"));
            int timeout = Integer.parseInt(ConfigReader.getConfig("Redis.xml", "redis_sharded", "common", "timeout"));
            int maxWait = Integer.parseInt(ConfigReader.getConfig("Redis.xml", "redis_sharded", "common", "max_wait"));
            logger.info("Max active: " + maxActive);
            logger.info("Max idle: " + maxIdle);
            logger.info("Timeout: " + timeout);
            logger.info("Max wait: " + maxWait);

            Dom dom = ConfigReader.getDom("Redis.xml");
//            Dom root = dom.element("Root");
            Dom redisSharded = dom.element("redis_sharded");
            List<Dom> shardListDom = redisSharded.elements("shard");
            for (Dom shardDom : shardListDom) {
                String host = shardDom.elementText("host");
                String port = shardDom.elementText("port");
                JedisShardInfo shard = new JedisShardInfo(host, Integer.parseInt(port), timeout);
                shardList.add(shard);
                logger.info("Add redis shard --- " + host + " " + port);
            }

            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxActive(maxActive);
            poolConfig.setMaxIdle(maxIdle);
            poolConfig.setMaxWait(maxWait);

            pool = new ShardedJedisPool(poolConfig, shardList);
            logger.info("Redis sharded pool init finished.");
        }
    }

    public synchronized static RedisShardedPoolResourceManager getInstance() {
        return InnerHolder.INSTANCE;
    }

    private static class InnerHolder {
        static final RedisShardedPoolResourceManager INSTANCE = new RedisShardedPoolResourceManager();
    }

    public ShardedJedis getCache(int index) {
        ShardedJedis shardedJedis = pool.getResource();
        Collection<Jedis> js = shardedJedis.getAllShards();
        for (Jedis jedis : js) {
            jedis.select(index);
        }
        return shardedJedis;
    }

    public void returnResource(ShardedJedis shardedJedis) {
        if (shardedJedis != null) {
            pool.returnResource(shardedJedis);
        }
    }

    public void returnBrokenResource(ShardedJedis shardedJedis) {
        if (shardedJedis != null) {
            pool.returnBrokenResource(shardedJedis);
        }
    }

    public void destory() {
        if (pool != null) {
            pool.destroy();
        }
    }
}
