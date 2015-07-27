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
import java.util.concurrent.locks.Lock;

import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.enums.EnumSyncAction;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.exceptions.LoadConfigException;
import org.pinus4j.generator.IIdGenerator;
import org.pinus4j.task.ITask;
import org.pinus4j.task.TaskFuture;
import org.pinus4j.transaction.enums.EnumTransactionIsolationLevel;

/**
 * main api of pinus.
 * 
 * @author duanbn
 */
public interface PinusClient {

    /**********************************************************
     * 事务相关
     *********************************************************/
    /**
     * default transaction isolation level is read_commited
     */
    void beginTransaction();

    void beginTransaction(EnumTransactionIsolationLevel txLevel);

    void commit();

    void rollback();

    /**********************************************************
     * 数据处理相关
     *********************************************************/
    /**
     * 提交一个数据处理任务.
     * 
     * @param task 处理任务
     * @param clazz 数据对象的Class
     * @return
     */
    <T> TaskFuture submit(ITask<T> task, Class<T> clazz);

    /**
     * 提交一个数据处理任务. 可以设置一个查询条件，只处理符合查询条件的数据
     * 
     * @param task 处理任务
     * @param clazz 数据对象的Class
     * @param query 查询条件
     * @return
     */
    <T> TaskFuture submit(ITask<T> task, Class<T> clazz, IQuery query);

    /**********************************************************
     * 数据操作相关
     *********************************************************/
    /**
     * 保存单个数据对象.
     * 
     * @param entity 被@Table标注的数据对象
     */
    public void save(Object entity);

    /**
     * 保存批量数据对象.
     * 
     * @param entityList
     */
    public void saveBatch(List<Object> entityList);

    public void update(Object entity);

    public void updateBatch(List<Object> entityList);

    public void delete(Object entity);

    public void delete(List<Object> entityList);

    public void load(Object entity);

    public void load(Object entity, boolean useCache);

    public void load(Object entity, EnumDBMasterSlave masterSlave);

    public void load(Object entity, boolean useCache, EnumDBMasterSlave masterSlave);

    public IQuery createQuery(Class<?> clazz);

    public <T> List<T> findBySQL(SQL sql);

    /**********************************************************
     * other相关
     *********************************************************/
    /**
     * 创建一个分布式锁.
     * 
     * @param lockName 锁名称
     * @return
     */
    Lock createLock(String lockName);

    /**
     * 设置ID生成器.
     * 
     * @param idGenerator
     */
    void setIdGenerator(IIdGenerator idGenerator);

    /**
     * 获取ID生成器
     * 
     * @return ID生成器
     */
    IIdGenerator getIdGenerator();

    /**
     * 获取当前使用的数据库集群.
     * 
     * @return 数据库集群
     */
    IDBCluster getDBCluster();

    /**
     * 生成全局唯一的int id. 对一个数据对象的集群全局唯一id.
     * 
     * @param name
     * @return 单个数据对象的集群全局唯一id
     * @throws DBOperationException 生成id失败
     */
    int genClusterUniqueIntId(String name);

    /**
     * 生成全局唯一的long id. 对一个数据对象的集群全局唯一id.
     * 
     * @param clusterName 数据库集群名
     * @param clazz 数据对象class
     * @return 单个数据对象的集群全局唯一id
     * @throws DBOperationException 生成id失败
     */
    long genClusterUniqueLongId(String name);

    /**
     * 批量生成全局唯一主键.
     * 
     * @param clusterName 数据库集群名
     * @param clazz 数据对象class
     * @param batchSize 批量数
     */
    long[] genClusterUniqueLongIdBatch(String name, int batchSize);

    /**
     * 批量生成全局唯一主键.
     * 
     * @param clusterName 数据库集群名
     * @param clazz 数据对象class
     * @param batchSize 批量数
     */
    int[] genClusterUniqueIntIdBatch(String name, int batchSize);

    /**
     * 初始化集群客户端.
     */
    void init() throws LoadConfigException;

    /**
     * 关闭存储.
     */
    void destroy();

    /**
     * 设置数据表同步动作.
     * 
     * @param syncAction
     */
    void setSyncAction(EnumSyncAction syncAction);

    /**
     * 设置扫描的实体对象包. 用户加载分表信息和自动创建数据表.
     * 
     * @param scanPackage
     */
    void setScanPackage(String scanPackage);

}
