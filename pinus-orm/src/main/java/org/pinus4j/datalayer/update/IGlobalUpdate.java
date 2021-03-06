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

package org.pinus4j.datalayer.update;

import java.util.List;

import org.pinus4j.exceptions.DBOperationException;

public interface IGlobalUpdate extends IDataUpdate {

    /**
	 * 保存数据到集群全局库.
	 * 
	 * @param entity
	 *            数据对象
	 * @param clusterName
	 *            集群名
	 * 
	 * @return 主键
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	public Number globalSave(Object entity, String clusterName);

	/**
	 * 批量保存数据到全局库.
	 * 
	 * @param entities
	 *            批量数据对象
	 * @param clusterName
	 *            集群名
	 * 
	 * @return 主键
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	public Number[] globalSaveBatch(List<? extends Object> entities, String clusterName);

    /**
	 * 更新全局库
	 * 
	 * @param entity
	 *            数据对象
	 * @param clusterName
	 *            集群名称
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	public void globalUpdate(Object entity, String clusterName);

	/**
	 * 批量更新全局库
	 * 
	 * @param entities
	 *            批量更新数据
	 * @param clusterName
	 *            集群名
	 * 
	 * @throws DBOperationException
	 *             操作失败
	 */
	public void globalUpdateBatch(List<? extends Object> entities, String clusterName);

	/**
	 * 删除全局库
	 * 
	 * @param pk
	 * @param shardingValue
	 * @param clazz
	 */
	public void globalRemoveByPk(Number pk, Class<?> clazz, String clusterName);

	/**
	 * 批量删除全局库
	 * 
	 * @param pks
	 * @param shardingValue
	 * @param clazz
	 */
	public void globalRemoveByPks(List<? extends Number> pks, Class<?> clazz, String clusterName);

}
