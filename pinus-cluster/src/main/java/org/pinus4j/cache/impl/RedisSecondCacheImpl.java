package org.pinus4j.cache.impl;

import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.utils.IOUtil;
import org.pinus4j.utils.SecurityUtil;
import org.pinus4j.utils.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RedisSecondCacheImpl extends AbstractRedisCache implements ISecondCache {

    public RedisSecondCacheImpl(String address, int expire) {
        super(address, expire);
    }

    @Override
    public void putGlobal(String whereSql, String clusterName, String tableName, List data) {
        if (StringUtils.isBlank(whereSql)) {
            return;
        }

        ShardedJedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            String cacheKey = _buildGlobalCacheKey(whereSql, clusterName, tableName);

            jedis.set(cacheKey.getBytes(), IOUtil.getBytes(data));
            jedis.expire(cacheKey.getBytes(), expire);

            if (LOG.isDebugEnabled()) {
                LOG.debug("[SECOND CACHE] - put to cache done, key: " + cacheKey);
            }
        } catch (Exception e) {
            LOG.warn("operate second cache failure");
        } finally {
            jedisPool.returnResourceObject(jedis);
        }
    }

    @Override
    public List getGlobal(String whereSql, String clusterName, String tableName) {
        if (StringUtils.isBlank(whereSql)) {
            return null;
        }

        ShardedJedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            String cacheKey = _buildGlobalCacheKey(whereSql, clusterName, tableName);
            List data = IOUtil.getObject(jedis.get(cacheKey.getBytes()), List.class);

            if (LOG.isDebugEnabled() && data != null) {
                LOG.debug("[SECOND CACHE] -  key " + cacheKey + " hit");
            }

            return data;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.warn("operate second cache failure");
        } finally {
            jedisPool.returnResourceObject(jedis);
        }

        return null;
    }

    @Override
    public void removeGlobal(String clusterName, String tableName) {
        ShardedJedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            List<String> keys = new ArrayList<String>();
            Collection<Jedis> shards = jedis.getAllShards();
            String cacheKey = _buildGlobalCacheKey(null, clusterName, tableName);
            for (Jedis shard : shards) {
                keys.addAll(shard.keys(cacheKey));

                shard.del(keys.toArray(new String[0]));
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("[SECOND CACHE] - " + cacheKey + " clean");
            }
        } catch (Exception e) {
            LOG.warn("remove second cache failure");
        } finally {
            jedisPool.returnResourceObject(jedis);
        }
    }

    @Override
    public void put(String whereSql, ShardingDBResource db, List data) {
        if (StringUtils.isBlank(whereSql)) {
            return;
        }

        ShardedJedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            String cacheKey = _buildShardingCacheKey(whereSql, db);
            jedis.set(cacheKey.getBytes(), IOUtil.getBytes(data));
            jedis.expire(cacheKey.getBytes(), expire);

            if (LOG.isDebugEnabled()) {
                LOG.debug("[SECOND CACHE] - put to cache done, key: " + cacheKey);
            }
        } catch (Exception e) {
            LOG.warn("operate second cache failure");
        } finally {
            jedisPool.returnResourceObject(jedis);
        }
    }

    @Override
    public List get(String whereSql, ShardingDBResource db) {
        if (StringUtils.isBlank(whereSql)) {
            return null;
        }

        ShardedJedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            String cacheKey = _buildShardingCacheKey(whereSql, db);
            List data = IOUtil.getObject(jedis.get(cacheKey.getBytes()), List.class);

            if (LOG.isDebugEnabled() && data != null) {
                LOG.debug("[SECOND CACHE] -  key " + cacheKey + " hit");
            }

            return data;
        } catch (Exception e) {
            LOG.warn("operate second cache failure");
        } finally {
            jedisPool.returnResourceObject(jedis);
        }

        return null;
    }

    @Override
    public void remove(ShardingDBResource db) {
        ShardedJedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            List<String> keys = new ArrayList<String>();
            Collection<Jedis> shards = jedis.getAllShards();
            String cacheKey = _buildShardingCacheKey(null, db);
            for (Jedis shard : shards) {
                keys.addAll(shard.keys(cacheKey));

                shard.del(keys.toArray(new String[0]));
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("[SECOND CACHE] - " + cacheKey + " clean");
            }
        } catch (Exception e) {
            LOG.warn("remove second cache failure");
        } finally {
            jedisPool.returnResourceObject(jedis);
        }
    }

    /**
     * global second cache key. sec.[clustername].[tablename].hashCode
     */
    private String _buildGlobalCacheKey(String whereSql, String clusterName, String tableName) {
        StringBuilder cacheKey = new StringBuilder("sec.");
        cacheKey.append(clusterName).append(".");
        cacheKey.append(tableName).append(".");
        if (StringUtils.isNotBlank(whereSql))
            cacheKey.append(SecurityUtil.md5(whereSql));
        else
            cacheKey.append("*");
        return cacheKey.toString();
    }

    /**
     * sharding second cache key. sec.[clustername].[startend].[tablename +
     * tableIndex].hashCode
     */
    private String _buildShardingCacheKey(String whereSql, ShardingDBResource shardingDBResource) {
        StringBuilder cacheKey = new StringBuilder("sec.");
        cacheKey.append(shardingDBResource.getClusterName());
        cacheKey.append(".");
        cacheKey.append(shardingDBResource.getDbName());
        cacheKey.append(".");
        cacheKey.append(shardingDBResource.getRegionCapacity());
        cacheKey.append(".");
        cacheKey.append(shardingDBResource.getTableName()).append(shardingDBResource.getTableIndex());
        cacheKey.append(".");
        if (StringUtils.isNotBlank(whereSql))
            cacheKey.append(SecurityUtil.md5(whereSql));
        else
            cacheKey.append("*");
        return cacheKey.toString();
    }

}
