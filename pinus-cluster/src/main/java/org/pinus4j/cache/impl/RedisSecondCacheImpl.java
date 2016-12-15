/**
 * Copyright 2014 Duan Bingnan
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pinus4j.cache.impl;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.utils.IOUtil;
import org.pinus4j.utils.SecurityUtil;
import org.pinus4j.utils.StringUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;

public class RedisSecondCacheImpl extends AbstractRedisCache implements ISecondCache {

    public RedisSecondCacheImpl(String address, int expire) {
        super(address, expire);
    }

    @Override
    public void putGlobal(String whereKey, String clusterName, String tableName, List data) {
        if (StringUtil.isBlank(whereKey) || data == null || data.isEmpty()) {
            return;
        }

        ShardedJedis redisClient = null;
        try {
            redisClient = jedisPool.getResource();

            String cacheKey = _buildGlobalCacheKey(whereKey, clusterName, tableName);

            redisClient.set(cacheKey.getBytes(), IOUtil.getBytes(data));
            redisClient.expire(cacheKey.getBytes(), expire);

            if (LOG.isDebugEnabled()) {
                LOG.debug("[SECOND CACHE] - put to cache done, key: " + cacheKey);
            }
        } catch (Exception e) {
            LOG.warn("operate second cache failure");
        } finally {
            if (redisClient != null)
                redisClient.close();
        }
    }

    @Override
    public List getGlobal(String whereKey, String clusterName, String tableName) {
        if (StringUtil.isBlank(whereKey)) {
            return null;
        }

        ShardedJedis redisClient = null;
        try {
            redisClient = jedisPool.getResource();

            String cacheKey = _buildGlobalCacheKey(whereKey, clusterName, tableName);
            List data = IOUtil.getObject(redisClient.get(cacheKey.getBytes()), List.class);

            if (LOG.isDebugEnabled() && data != null) {
                LOG.debug("[SECOND CACHE] -  key " + cacheKey + " hit");
            }

            return data;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.warn("operate second cache failure");
        } finally {
            if (redisClient != null)
                redisClient.close();
        }

        return null;
    }

    @Override
    public void removeGlobal(String clusterName, String tableName) {
        ShardedJedis redisClient = null;
        String cacheKey = _buildGlobalCacheKey(null, clusterName, tableName);
        try {
            redisClient = jedisPool.getResource();
            Collection<Jedis> shards = redisClient.getAllShards();
            Set<String> keys = null;
            for (Jedis shard : shards) {
                keys = shard.keys(cacheKey);

                if (keys != null && !keys.isEmpty())
                    shard.del(keys.toArray(new String[keys.size()]));
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("[SECOND CACHE] - " + cacheKey + " clean");
            }
        } catch (Exception e) {
            LOG.warn("remove second cache failure");
        } finally {
            if (redisClient != null)
                redisClient.close();
        }
    }

    @Override
    public void put(String whereKey, ShardingDBResource db, List data) {
        if (StringUtil.isBlank(whereKey) || data == null || data.isEmpty()) {
            return;
        }

        ShardedJedis redisClient = null;
        try {
            redisClient = jedisPool.getResource();
            String cacheKey = _buildShardingCacheKey(whereKey, db);
            redisClient.set(cacheKey.getBytes(), IOUtil.getBytes(data));
            redisClient.expire(cacheKey.getBytes(), expire);

            if (LOG.isDebugEnabled()) {
                LOG.debug("[SECOND CACHE] - put to cache done, key: " + cacheKey);
            }
        } catch (Exception e) {
            LOG.warn("operate second cache failure");
        } finally {
            if (redisClient != null)
                redisClient.close();
        }
    }

    @Override
    public List get(String whereKey, ShardingDBResource db) {
        if (StringUtil.isBlank(whereKey)) {
            return null;
        }

        ShardedJedis redisClient = null;
        try {
            redisClient = jedisPool.getResource();
            String cacheKey = _buildShardingCacheKey(whereKey, db);
            List data = IOUtil.getObject(redisClient.get(cacheKey.getBytes()), List.class);

            if (LOG.isDebugEnabled() && data != null) {
                LOG.debug("[SECOND CACHE] -  key " + cacheKey + " hit");
            }

            return data;
        } catch (Exception e) {
            LOG.warn("operate second cache failure");
        } finally {
            if (redisClient != null)
                redisClient.close();
        }

        return null;
    }

    @Override
    public void remove(ShardingDBResource db) {
        ShardedJedis redisClient = null;
        String cacheKey = _buildShardingCacheKey(null, db);
        try {
            redisClient = jedisPool.getResource();
            Collection<Jedis> shards = redisClient.getAllShards();

            Set<String> keys = null;
            for (Jedis shard : shards) {
                keys = shard.keys(cacheKey);

                if (keys != null && !keys.isEmpty())
                    shard.del(keys.toArray(new String[keys.size()]));
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("[SECOND CACHE] - " + cacheKey + " clean");
            }
        } catch (Exception e) {
            LOG.warn("remove second cache failure " + cacheKey);
        } finally {
            if (redisClient != null)
                redisClient.close();
        }
    }

    /**
     * global second cache key. sec.[clustername].[tablename].hashCode
     */
    private String _buildGlobalCacheKey(String whereKey, String clusterName, String tableName) {
        StringBuilder cacheKey = new StringBuilder("sec.");
        cacheKey.append(clusterName).append(".");
        cacheKey.append(tableName);
        cacheKey.append(".");
        cacheKey.append(getCacheVersion(tableName));
        cacheKey.append(".");
        if (StringUtil.isNotBlank(whereKey))
            cacheKey.append(SecurityUtil.md5(whereKey));
        else
            cacheKey.append("*");
        return cacheKey.toString();
    }

    /**
     * sharding second cache key. sec.[clustername].[startend].[tablename +
     * tableIndex].hashCode
     */
    private String _buildShardingCacheKey(String whereKey, ShardingDBResource shardingDBResource) {
        StringBuilder cacheKey = new StringBuilder("sec.");
        cacheKey.append(shardingDBResource.getClusterName());
        cacheKey.append(".");
        cacheKey.append(shardingDBResource.getDbName());
        cacheKey.append(".");
        cacheKey.append(shardingDBResource.getRegionCapacity());
        cacheKey.append(".");
        cacheKey.append(shardingDBResource.getTableName()).append(shardingDBResource.getTableIndex());
        cacheKey.append(".");
        cacheKey.append(getCacheVersion(shardingDBResource.getTableName()));
        cacheKey.append(".");
        if (StringUtil.isNotBlank(whereKey))
            cacheKey.append(SecurityUtil.md5(whereKey));
        else
            cacheKey.append("*");
        return cacheKey.toString();
    }

}
