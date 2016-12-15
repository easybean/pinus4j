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

package org.pinus4j.cluster.resources;

import java.sql.Connection;
import java.sql.SQLException;

import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.cluster.beans.DBRegionInfo;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.transaction.ITransaction;
import org.pinus4j.transaction.enums.EnumTransactionIsolationLevel;

/**
 * 表示一个数据分片资源.
 * 
 * @author duanbn
 */
public class ShardingDBResource extends AbstractXADBResource {

    private IResourceId       resId;

    /**
     * jdbc data source.
     */
    private Connection        connection;

    /**
     * cluster name.
     */
    private String            clusterName;

    /**
     * database name.
     */
    private String            dbName;

    /**
     * table name without index.
     */
    private String            tableName;

    /**
     * index of table
     */
    private int               tableIndex;

    private String            regionCapacity;

    private EnumDBMasterSlave masterSlave;

    private ShardingDBResource() {
    }

    public static ShardingDBResource valueOf(ITransaction tx, DBInfo dbInfo, DBRegionInfo regionInfo, String tableName,
                                             int tableIndex) throws SQLException {
        IResourceId resId = new DBResourceId(dbInfo.getClusterName(), dbInfo.getDbName(), regionInfo.getCapacity(),
                tableName, tableIndex, dbInfo.getMasterSlave());

        ShardingDBResource dbResource = null;

        if (tx != null && tx.isContain(resId)) {

            dbResource = (ShardingDBResource) tx.getDBResource(resId);

        } else {

            Connection conn = dbInfo.getDatasource().getConnection();
            conn.setAutoCommit(false);

            dbResource = new ShardingDBResource();

            dbResource.setId(resId);
            dbResource.setClusterName(dbInfo.getClusterName());
            dbResource.setDbName(dbInfo.getDbName());
            dbResource.setRegionCapacity(regionInfo.getCapacity());
            dbResource.setTableName(tableName);
            dbResource.setTableIndex(tableIndex);
            dbResource.setMasterSlave(dbInfo.getMasterSlave());

            dbResource.setConnection(conn);
        }

        return dbResource;
    }

    public void setId(IResourceId resId) {
        this.resId = resId;
    }

    @Override
    public IResourceId getId() {
        return this.resId;
    }

    @Override
    public void setTransactionIsolationLevel(EnumTransactionIsolationLevel txLevel) {
        try {
            this.connection.setTransactionIsolation(txLevel.getLevel());
        } catch (SQLException e) {
            throw new DBOperationException(e);
        }
    }

    @Override
    public Connection getConnection() {
        return this.connection;
    }

    public void setConnection(Connection conn) {
        this.connection = conn;
    }

    @Override
    public void commit() {
        try {
            this.connection.commit();
        } catch (SQLException e) {
            throw new DBOperationException(e);
        }
    }

    @Override
    public void rollback() {
        try {
            this.connection.rollback();
        } catch (SQLException e) {
            throw new DBOperationException(e);
        }
    }

    @Override
    public void close() {
        try {
            if (!this.connection.isClosed()) {
                this.connection.close();
            }
        } catch (SQLException e) {
            throw new DBOperationException(e);
        }
    }

    @Override
    public boolean isClosed() {
        try {
            return this.connection.isClosed();
        } catch (SQLException e) {
            throw new DBOperationException(e);
        }
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public boolean isGlobal() {
        return false;
    }

    @Override
    public EnumDBMasterSlave getMasterSlave() {
        return this.masterSlave;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getTableIndex() {
        return tableIndex;
    }

    public void setTableIndex(int tableIndex) {
        this.tableIndex = tableIndex;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getRegionCapacity() {
        return regionCapacity;
    }

    public void setRegionCapacity(String regionCapacity) {
        this.regionCapacity = regionCapacity;
    }

    public void setMasterSlave(EnumDBMasterSlave masterSlave) {
        this.masterSlave = masterSlave;
    }

    @Override
    public String toString() {
        return "ShardingDBResource [clusterName=" + clusterName + ", dbName=" + dbName + ", tableName=" + tableName
                + ", tableIndex=" + tableIndex + ", regionCapacity=" + regionCapacity + ", masterSlave=" + masterSlave
                + "]";
    }

}
