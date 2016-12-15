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

package org.pinus4j.api;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import javax.transaction.TransactionManager;

import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.impl.ResultSetableQueryImpl;
import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.cluster.IDBClusterBuilder;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.enums.EnumDB;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.enums.EnumSyncAction;
import org.pinus4j.cluster.impl.DefaultDBClusterBuilder;
import org.pinus4j.constant.Const;
import org.pinus4j.datalayer.IDataLayerBuilder;
import org.pinus4j.datalayer.JdbcDataLayerBuilder;
import org.pinus4j.datalayer.query.IGlobalQuery;
import org.pinus4j.datalayer.query.IShardingQuery;
import org.pinus4j.datalayer.update.IGlobalUpdate;
import org.pinus4j.datalayer.update.IShardingUpdate;
import org.pinus4j.entity.DefaultEntityMetaManager;
import org.pinus4j.entity.IEntityMetaManager;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.exceptions.DBClusterException;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.generator.IIdGenerator;
import org.pinus4j.task.ITask;
import org.pinus4j.task.TaskExecutor;
import org.pinus4j.task.TaskFuture;
import org.pinus4j.transaction.enums.EnumTransactionIsolationLevel;
import org.pinus4j.transaction.impl.BestEffortsOnePCJtaTransaction;
import org.pinus4j.transaction.impl.BestEffortsOnePCJtaTransactionManager;
import org.pinus4j.utils.BeansUtil;
import org.pinus4j.utils.CheckUtil;
import org.pinus4j.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * default main api implement.
 *
 * @author duanbn
 */
public class DefaultPinusClient implements PinusClient {

    /**
     * 日志.
     */
    public static final Logger LOG               = LoggerFactory.getLogger(DefaultPinusClient.class);

    /**
     * reference it self;
     */
    public static PinusClient  instance;

    /**
     * 数据库类型.
     */
    private EnumDB             enumDb            = EnumDB.MYSQL;

    /**
     * 同步数据表操作.
     */
    private EnumSyncAction     syncAction        = EnumSyncAction.CREATE;

    /**
     * 扫描数据对象的包. 数据对象是使用了@Table注解的javabean.
     */
    private String             scanPackage;

    /**
     * id generator.
     */
    private IIdGenerator       idGenerator;

    /**
     * ref of db cluster.
     */
    private IDBCluster         dbCluster;

    /**
     * transaction manager.
     */
    private TransactionManager txManager;

    /**
     * global updater.
     */
    private IGlobalUpdate      globalUpdater;

    /**
     * global query.
     */
    private IGlobalQuery       globalQuery;

    /**
     * 分库分表更新实现.
     */
    private IShardingUpdate    shardingUpdater;

    /**
     * sharding query.
     */
    private IShardingQuery     shardingQuery;

    private IEntityMetaManager entityMetaManager = DefaultEntityMetaManager.getInstance();

    /**
     * init method
     */
    public void init() {
        IDBClusterBuilder dbClusterBuilder = new DefaultDBClusterBuilder();
        dbClusterBuilder.setScanPackage(this.scanPackage);
        dbClusterBuilder.setSyncAction(this.syncAction);
        dbClusterBuilder.setDbType(this.enumDb);
        this.dbCluster = dbClusterBuilder.build();

        // set id generator
        this.idGenerator = this.dbCluster.getIdGenerator();

        // set transaction manager
        this.txManager = this.dbCluster.getTransactionManager();

        //
        // 初始化分库分表增删改查实现.
        //
        IDataLayerBuilder dataLayerBuilder = JdbcDataLayerBuilder.valueOf(dbCluster);
        dataLayerBuilder.setPrimaryCache(this.dbCluster.getPrimaryCache());
        dataLayerBuilder.setSecondCache(this.dbCluster.getSecondCache());

        this.globalUpdater = dataLayerBuilder.buildGlobalUpdate(this.dbCluster.getIdGenerator());
        this.globalQuery = dataLayerBuilder.buildGlobalQuery();

        this.shardingUpdater = dataLayerBuilder.buildShardingUpdate(this.dbCluster.getIdGenerator());
        this.shardingQuery = dataLayerBuilder.buildShardingQuery();

        // FashionEntity dependency this.
        instance = this;
    }

    @Override
    public void destroy() {
        // close database cluster.
        try {
            this.dbCluster.shutdown();
        } catch (DBClusterException e) {
            throw new RuntimeException(e);
        }

    }

    // ////////////////////////////////////////////////////////
    // 事务相关
    // ////////////////////////////////////////////////////////
    @Override
    public void beginTransaction() {
        beginTransaction(EnumTransactionIsolationLevel.READ_COMMITTED);
    }

    @Override
    public void beginTransaction(EnumTransactionIsolationLevel txLevel) {
        ((BestEffortsOnePCJtaTransactionManager) this.txManager).setTransactionIsolationLevel(txLevel);
        try {
            this.txManager.begin();
        } catch (Exception e) {
            throw new DBOperationException(e);
        }
    }

    @Override
    public void commit() {
        try {
            this.txManager.commit();
        } catch (Exception e) {
            throw new DBOperationException(e);
        }
    }

    @Override
    public void rollback() {
        try {
            this.txManager.rollback();
        } catch (Exception e) {
            throw new DBOperationException(e);
        }
    }

    @Override
    public void flush() {
        try {
            BestEffortsOnePCJtaTransaction localTx = (BestEffortsOnePCJtaTransaction) this.txManager.getTransaction();

            if (localTx != null)
                localTx.flush();
        } catch (Exception e) {
            throw new DBOperationException(e);
        }
    }

    // ////////////////////////////////////////////////////////
    // 数据处理相关
    // ////////////////////////////////////////////////////////
    @Override
    public <T> TaskFuture submit(ITask<T> task, Class<T> clazz) {
        TaskExecutor<T> taskExecutor = new TaskExecutor<T>(clazz, this.dbCluster);
        return taskExecutor.execute(task);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <T> TaskFuture submit(ITask<T> task, Class<T> clazz, IQuery<T> query) {
        TaskExecutor taskExecutor = new TaskExecutor(clazz, this.dbCluster);
        return taskExecutor.execute(task, query);
    }

    // ////////////////////////////////////////////////////////
    // 数据操作相关
    // ////////////////////////////////////////////////////////
    @Override
    public void save(Object entity) {
        if (entity == null) {
            throw new IllegalArgumentException("param should not be null");
        }

        Class<?> clazz = entity.getClass();

        if (entityMetaManager.isShardingEntity(clazz)) {

            CheckUtil.checkShardingEntity(entity);

            IShardingKey<?> sk = entityMetaManager.getShardingKey(entity);
            CheckUtil.checkShardingKey(sk);

            this.shardingUpdater.save(entity, sk);

        } else {

            CheckUtil.checkGlobalEntity(entity);
            String clusterName = entityMetaManager.getClusterName(entity.getClass());
            CheckUtil.checkClusterName(clusterName);

            this.globalUpdater.save(entity, clusterName);

        }
    }

    @Override
    public void saveBatch(List<? extends Object> entityList) {
        saveBatch(entityList, true);
    }

    @Override
    public void saveBatch(List<? extends Object> entityList, boolean autoGeneratedKeys) {
        if (entityList == null) {
            throw new IllegalArgumentException("param should not be null");
        }

        List<Object> globalList = Lists.newArrayList();
        List<Object> shardingList = Lists.newArrayList();

        for (Object entity : entityList) {
            if (entityMetaManager.isShardingEntity(entity.getClass())) {
                shardingList.add(entity);
            } else {
                globalList.add(entity);
            }
        }

        // handle global entity list.
        if (!globalList.isEmpty()) {
            Map<String, List<Object>> clusterEntityMap = Maps.newHashMap();

            String clusterName = null;
            for (Object globalEntity : globalList) {
                clusterName = entityMetaManager.getClusterName(globalEntity.getClass());
                List<Object> theSameClusterNameList = clusterEntityMap.get(clusterName);
                if (theSameClusterNameList != null) {
                    theSameClusterNameList.add(globalEntity);
                } else {
                    theSameClusterNameList = Lists.newArrayList(globalEntity);
                    clusterEntityMap.put(clusterName, theSameClusterNameList);
                }
            }

            for (Map.Entry<String, List<Object>> entry : clusterEntityMap.entrySet()) {
                this.globalUpdater.saveBatch(entry.getValue(), entry.getKey(), autoGeneratedKeys);
            }
        }

        // handle sharding entity list.
        if (!shardingList.isEmpty()) {
            Map<IShardingKey<?>, List<Object>> theSameShardingKeyMap = Maps.newHashMap();

            IShardingKey<?> shardingKey = null;
            for (Object shardingEntity : shardingList) {
                shardingKey = entityMetaManager.getShardingKey(shardingEntity);
                List<Object> theSameShardingKeyList = theSameShardingKeyMap.get(shardingKey);
                if (theSameShardingKeyList != null) {
                    theSameShardingKeyList.add(shardingEntity);
                } else {
                    theSameShardingKeyList = Lists.newArrayList(shardingEntity);
                    theSameShardingKeyMap.put(shardingKey, theSameShardingKeyList);
                }
            }

            for (Map.Entry<IShardingKey<?>, List<Object>> entry : theSameShardingKeyMap.entrySet()) {
                this.shardingUpdater.saveBatch(entry.getValue(), entry.getKey(), autoGeneratedKeys);
            }
        }

    }

    @Override
    public void update(Object entity) {
        if (entity == null) {
            throw new IllegalArgumentException("param should not be null");
        }

        Class<?> clazz = entity.getClass();

        if (entityMetaManager.isShardingEntity(clazz)) {

            CheckUtil.checkShardingEntity(entity);

            IShardingKey<?> sk = entityMetaManager.getShardingKey(entity);
            CheckUtil.checkShardingKey(sk);

            this.shardingUpdater.update(entity, sk);

        } else {

            CheckUtil.checkGlobalEntity(entity);

            String clusterName = entityMetaManager.getClusterName(entity.getClass());
            CheckUtil.checkClusterName(clusterName);

            this.globalUpdater.update(entity, clusterName);

        }
    }

    @Override
    public void updateBatch(List<? extends Object> entityList) {
        if (entityList == null) {
            throw new IllegalArgumentException("param should not be null");
        }

        List<Object> globalList = Lists.newArrayList();
        List<Object> shardingList = Lists.newArrayList();

        for (Object entity : entityList) {
            if (entityMetaManager.isShardingEntity(entity.getClass())) {
                shardingList.add(entity);
            } else {
                globalList.add(entity);
            }
        }

        // handle global entity list.
        if (!globalList.isEmpty()) {
            Map<String, List<Object>> theSameClusterNameMap = Maps.newHashMap();

            String clusterName = null;
            for (Object globalEntity : globalList) {
                clusterName = entityMetaManager.getClusterName(globalEntity.getClass());
                List<Object> theSameClusterNameList = theSameClusterNameMap.get(clusterName);
                if (theSameClusterNameList != null) {
                    theSameClusterNameList.add(globalEntity);
                } else {
                    theSameClusterNameList = Lists.newArrayList(globalEntity);
                    theSameClusterNameMap.put(clusterName, theSameClusterNameList);
                }
            }

            for (Map.Entry<String, List<Object>> entry : theSameClusterNameMap.entrySet()) {
                this.globalUpdater.updateBatch(entry.getValue(), entry.getKey());
            }
        }

        // handle sharding entity list.
        if (!shardingList.isEmpty()) {
            Map<IShardingKey<?>, List<Object>> theSameShardingKeyMap = Maps.newHashMap();

            IShardingKey<?> shardingKey = null;
            for (Object shardingEntity : shardingList) {
                shardingKey = entityMetaManager.getShardingKey(shardingEntity);
                List<Object> theSameShardingKeyList = theSameShardingKeyMap.get(shardingKey);
                if (theSameShardingKeyList != null) {
                    theSameShardingKeyList.add(shardingEntity);
                } else {
                    theSameShardingKeyList = Lists.newArrayList(shardingEntity);
                    theSameShardingKeyMap.put(shardingKey, theSameShardingKeyList);
                }
            }

            for (Map.Entry<IShardingKey<?>, List<Object>> entry : theSameShardingKeyMap.entrySet()) {
                this.shardingUpdater.updateBatch(entry.getValue(), entry.getKey());
            }
        }
    }

    @Override
    public void delete(Object entity) {
        if (entity == null) {
            throw new IllegalArgumentException("param should not be null");
        }

        Class<?> clazz = entity.getClass();

        if (entityMetaManager.isShardingEntity(clazz)) {

            CheckUtil.checkShardingEntity(entity);

            EntityPK entityPk = entityMetaManager.getEntityPK(entity);
            IShardingKey<?> shardingKey = entityMetaManager.getShardingKey(entity);

            this.shardingUpdater.removeByPk(entityPk, shardingKey, clazz);

        } else {

            CheckUtil.checkGlobalEntity(entity);

            EntityPK entityPk = entityMetaManager.getEntityPK(entity);
            String clusterName = entityMetaManager.getClusterName(clazz);

            this.globalUpdater.removeByPk(entityPk, clazz, clusterName);

        }
    }

    @Override
    public void delete(List<? extends Object> entityList) {
        if (entityList == null) {
            throw new IllegalArgumentException("param should not be null");
        }

        List<Object> globalList = Lists.newArrayList();
        List<Object> shardingList = Lists.newArrayList();

        for (Object entity : entityList) {
            if (entityMetaManager.isShardingEntity(entity.getClass())) {
                shardingList.add(entity);
            } else {
                globalList.add(entity);
            }
        }

        // handle global entity list.
        if (!globalList.isEmpty()) {

            // filter the same cluster name
            Map<String, List<Object>> theSameClusterNameMap = Maps.newHashMap();

            String clusterName = null;
            for (Object globalEntity : globalList) {
                clusterName = entityMetaManager.getClusterName(globalEntity.getClass());
                List<Object> theSameClusterNameList = theSameClusterNameMap.get(clusterName);
                if (theSameClusterNameList != null) {
                    theSameClusterNameList.add(globalEntity);
                } else {
                    theSameClusterNameList = Lists.newArrayList(globalEntity);
                    theSameClusterNameMap.put(clusterName, theSameClusterNameList);
                }
            }

            // filter the same class
            for (Map.Entry<String, List<Object>> sameClusterNameEntry : theSameClusterNameMap.entrySet()) {
                Map<Class<?>, List<Object>> theSameClassMap = Maps.newHashMap();
                Class<?> clazz = null;
                for (Object sameClusterNameEntity : sameClusterNameEntry.getValue()) {
                    clazz = sameClusterNameEntity.getClass();
                    List<Object> theSameClassList = theSameClassMap.get(clazz);
                    if (theSameClassList != null) {
                        theSameClassList.add(sameClusterNameEntity);
                    } else {
                        theSameClassMap.put(clazz, Lists.newArrayList(sameClusterNameEntity));
                    }

                    // do delete
                    for (Map.Entry<Class<?>, List<Object>> sameClassEntry : theSameClassMap.entrySet()) {
                        List<EntityPK> pks = Lists.newArrayList();
                        for (Object sameClassEntity : sameClassEntry.getValue()) {
                            pks.add(entityMetaManager.getEntityPK(sameClassEntity));
                        }
                        this.globalUpdater.removeByPks(pks, sameClassEntry.getKey(), sameClusterNameEntry.getKey());
                    }
                }
            }
        }

        // handle sharding entity list.
        if (!shardingList.isEmpty()) {

            // filter the same sharding key
            Map<IShardingKey<?>, List<Object>> theSameShardingKeyMap = Maps.newHashMap();

            IShardingKey<?> shardingKey = null;
            for (Object shardingEntity : shardingList) {
                shardingKey = entityMetaManager.getShardingKey(shardingEntity);
                List<Object> theSameShardingKeyList = theSameShardingKeyMap.get(shardingKey);
                if (theSameShardingKeyList != null) {
                    theSameShardingKeyList.add(shardingEntity);
                } else {
                    theSameShardingKeyList = Lists.newArrayList(shardingEntity);
                    theSameShardingKeyMap.put(shardingKey, theSameShardingKeyList);
                }
            }

            // filter the same class
            for (Map.Entry<IShardingKey<?>, List<Object>> sameShardingKeyEntry : theSameShardingKeyMap.entrySet()) {
                Map<Class<?>, List<Object>> theSameClassMap = Maps.newHashMap();
                Class<?> clazz = null;
                for (Object sameShardingKeyEntity : sameShardingKeyEntry.getValue()) {
                    clazz = sameShardingKeyEntity.getClass();
                    List<Object> theSameClassList = theSameClassMap.get(clazz);
                    if (theSameClassList != null) {
                        theSameClassList.add(sameShardingKeyEntity);
                    } else {
                        theSameClassMap.put(clazz, Lists.newArrayList(sameShardingKeyEntity));
                    }

                    // do delete
                    for (Map.Entry<Class<?>, List<Object>> sameClassEntry : theSameClassMap.entrySet()) {
                        List<EntityPK> pks = Lists.newArrayList();
                        for (Object sameClassEntity : sameClassEntry.getValue()) {
                            pks.add(entityMetaManager.getEntityPK(sameClassEntity));
                        }
                        this.shardingUpdater.removeByPks(pks, sameShardingKeyEntry.getKey(), sameClassEntry.getKey());
                    }
                }
            }

        }

    }

    @Override
    public void load(Object entity) {
        load(entity, true, EnumDBMasterSlave.AUTO);
    }

    @Override
    public void load(Object entity, boolean useCache) {
        load(entity, useCache, EnumDBMasterSlave.AUTO);
    }

    @Override
    public void load(Object entity, EnumDBMasterSlave masterSlave) {
        load(entity, true, masterSlave);
    }

    @Override
    public void load(Object entity, boolean useCache, EnumDBMasterSlave masterSlave) {
        if (entity == null) {
            throw new IllegalArgumentException("param should not be null");
        }

        EntityPK entityPk = entityMetaManager.getEntityPK(entity);

        Object loadEntity = null;
        Class<?> clazz = entity.getClass();
        if (entityMetaManager.isShardingEntity(clazz)) {
            IShardingKey<?> shardingKey = entityMetaManager.getShardingKey(entity);
            loadEntity = this.shardingQuery.findByPk(entityPk, shardingKey, clazz, useCache, masterSlave);
        } else {
            loadEntity = this.globalQuery.findByPk(entityPk, clazz, useCache, masterSlave);
        }

        if (loadEntity == null) {
            throw new DBOperationException("找不到记录, pk=" + entityPk);
        }

        try {
            BeansUtil.copyProperties(loadEntity, entity);
        } catch (Exception e) {
            throw new DBOperationException(e);
        }

    }

    @Override
    public <T> IQuery<T> createQuery(Class<T> clazz) {
        ResultSetableQueryImpl<T> query = new ResultSetableQueryImpl<T>(clazz);
        query.setGlobalQuery(this.globalQuery);
        query.setShardingQuery(this.shardingQuery);
        return query;
    }

    @Override
    public List<Map<String, Object>> findBySQL(SQL sql, Class<?> clazz) {
        if (sql == null) {
            throw new IllegalArgumentException("param sql should not be null");
        }
        if (clazz == null) {
            throw new IllegalArgumentException("param class should not be null");
        }

        CheckUtil.checkSQL(sql);
        CheckUtil.checkClass(clazz);

        String clusterName = entityMetaManager.getClusterName(clazz);

        if (entityMetaManager.isShardingEntity(clazz)) {
            return this.shardingQuery.findBySql(sql, EnumDBMasterSlave.AUTO);
        } else {
            return this.globalQuery.findBySql(sql, clusterName, EnumDBMasterSlave.AUTO);
        }
    }

    @Override
    public List<Map<String, Object>> findBySQL(SQL sql, String clusterName) {
        if (sql == null) {
            throw new IllegalArgumentException("param sql should not be null");
        }

        CheckUtil.checkSQL(sql);

        return this.globalQuery.findBySql(sql, clusterName, EnumDBMasterSlave.AUTO);
    }

    @Override
    public IDBCluster getDBCluster() {
        return this.dbCluster;
    }

    @Override
    public IPrimaryCache getPrimaryCache() {
        return this.dbCluster.getPrimaryCache();
    }

    @Override
    public ISecondCache getSecondCache() {
        return this.dbCluster.getSecondCache();
    }

    @Override
    public int genClusterUniqueIntId(String name) {
        return this.idGenerator.genClusterUniqueIntId(Const.ZK_SEQUENCE, name);
    }

    @Override
    public long genClusterUniqueLongId(String name) {
        return this.idGenerator.genClusterUniqueLongId(Const.ZK_SEQUENCE, name);
    }

    @Override
    public long[] genClusterUniqueLongIdBatch(String name, int batchSize) {
        return this.idGenerator.genClusterUniqueLongIdBatch(Const.ZK_SEQUENCE, name, batchSize);
    }

    @Override
    public int[] genClusterUniqueIntIdBatch(String name, int batchSize) {
        return this.idGenerator.genClusterUniqueIntIdBatch(Const.ZK_SEQUENCE, name, batchSize);
    }

    @Override
    public Lock createLock(String lockName) {
        return this.dbCluster.createLock(lockName);
    }

    public EnumSyncAction getSyncAction() {
        return syncAction;
    }

    @Override
    public void setSyncAction(EnumSyncAction syncAction) {
        this.syncAction = syncAction;
    }

    public String getScanPackage() {
        return scanPackage;
    }

    @Override
    public void setScanPackage(String scanPackage) {
        if (StringUtil.isBlank(scanPackage)) {
            throw new IllegalArgumentException("参数错误，参数不能为空");
        }

        this.scanPackage = scanPackage;
    }
}
