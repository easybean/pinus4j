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

package org.pinus4j.cluster.beans;

/**
 * 基于数值的分库分表路由因子接口实现. 集群路由器会根据此对象的value值计算分库分表.
 * 
 * @author duanbn
 */
public class ShardingKey<T> implements IShardingKey<T>, Cloneable {

    /**
     * 集群数据库名称.
     */
    private String clusterName;

    /**
     * 分库分表因子.
     */
    private T      value;

    /**
     * 构造方法.
     * 
     * @param clusterName 数据库集群名
     * @param value 分库分表因子值
     */
    public ShardingKey(String clusterName, T value) {
        this.clusterName = clusterName;
        this.value = value;
    }

    /**
     * 构造方法
     */
    public static final <T> ShardingKey<T> valueOf(String clusterName, T value) {
        ShardingKey<T> sk = new ShardingKey<T>(clusterName, value);
        return sk;
    }

    @Override
    public String getClusterName() {
        return this.clusterName;
    }

    @Override
    public T getValue() {
        return this.value;
    }

    @Override
    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public String toString() {
        StringBuilder info = new StringBuilder();
        info.append("clusterName=").append(this.clusterName);
        info.append(", value=").append(this.value);
        return info.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((clusterName == null) ? 0 : clusterName.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ShardingKey other = (ShardingKey) obj;
        if (clusterName == null) {
            if (other.clusterName != null)
                return false;
        } else if (!clusterName.equals(other.clusterName))
            return false;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }

}
