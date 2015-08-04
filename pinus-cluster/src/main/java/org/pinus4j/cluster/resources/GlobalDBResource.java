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
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.pinus4j.cluster.beans.DBInfo;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.transaction.ITransaction;
import org.pinus4j.transaction.enums.EnumTransactionIsolationLevel;

/**
 * 全局数据资源.
 * 
 * @author duanbn
 * @since 1.1.0
 */
public class GlobalDBResource extends AbstractXADBResource {

    private IResourceId       resId;

    private Connection        conn;

    private String            clusterName;

    private String            dbName;

    private EnumDBMasterSlave masterSlave;

    //
    // database meta data.
    //
    private String            databaseProductName;
    private String            host;
    private String            catalog;

    private GlobalDBResource() {
    }

    /**
     * singleton
     * 
     * @param dbInfo
     * @return
     */
    public static IDBResource valueOf(ITransaction tx, DBInfo dbInfo, String tableName) throws SQLException {
        IResourceId resId = new DBResourceId(dbInfo.getClusterName(), dbInfo.getDbName(), tableName,
                dbInfo.getMasterSlave());

        GlobalDBResource dbResource = null;

        if (tx != null && tx.isContain(resId)) {

            dbResource = (GlobalDBResource) tx.getDBResource(resId);

        } else {

            dbResource = (GlobalDBResource) DBResourceCache.getGlobalDBResource(resId);

            Connection conn = dbInfo.getDatasource().getConnection();
            conn.setAutoCommit(false);

            if (dbResource == null) {
                dbResource = new GlobalDBResource();

                dbResource.setId(resId);
                dbResource.setClusterName(dbInfo.getClusterName());
                dbResource.setDbName(dbInfo.getDbName());
                dbResource.setMasterSlave(dbInfo.getMasterSlave());

                // get database meta info.
                DatabaseMetaData dbMeta = conn.getMetaData();
                String databaseProductName = dbMeta.getDatabaseProductName();
                String url = dbMeta.getURL().substring(13);
                String host = url.substring(0, url.indexOf("/"));
                String catalog = conn.getCatalog();

                dbResource.setDatabaseProductName(databaseProductName);
                dbResource.setHost(host);
                dbResource.setCatalog(catalog);

                DBResourceCache.putGlobalDBResource(resId, dbResource);
            }

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
            this.conn.setTransactionIsolation(txLevel.getLevel());
        } catch (SQLException e) {
            throw new DBOperationException(e);
        }
    }

    @Override
    public Connection getConnection() {
        return this.conn;
    }

    @Override
    public void commit() {
        try {
            this.conn.commit();
        } catch (SQLException e) {
            throw new DBOperationException(e);
        }
    }

    @Override
    public void rollback() {
        try {
            this.conn.rollback();
        } catch (SQLException e) {
            throw new DBOperationException(e);
        }
    }

    @Override
    public void close() {
        try {
            if (!this.conn.isClosed()) {
                this.conn.close();
            }
        } catch (SQLException e) {
            throw new DBOperationException(e);
        }
    }

    @Override
    public boolean isClosed() {
        try {
            return this.conn.isClosed();
        } catch (SQLException e) {
            throw new DBOperationException(e);
        }
    }

    @Override
    public boolean isGlobal() {
        return true;
    }

    @Override
    public EnumDBMasterSlave getMasterSlave() {
        return this.masterSlave;
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDatabaseProductName() {
        return databaseProductName;
    }

    public void setDatabaseProductName(String databaseProductName) {
        this.databaseProductName = databaseProductName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public void setConnection(Connection conn) {
        this.conn = conn;
    }

    public void setMasterSlave(EnumDBMasterSlave masterSlave) {
        this.masterSlave = masterSlave;
    }

}
