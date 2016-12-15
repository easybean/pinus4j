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

package org.pinus4j.cluster.cp.impl;

import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.pinus4j.exceptions.LoadConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * connection pool implement by dbcp
 * 
 * @author shanwei Jul 28, 2015 3:42:22 PM
 */
public class DBCPConnectionPoolImpl extends AbstractConnectionPool {

    public static Logger LOG = LoggerFactory.getLogger(DBCPConnectionPoolImpl.class);

    @Override
    public void releaseDataSource(DataSource datasource) {
        try {
            ((BasicDataSource) datasource).close();
        } catch (SQLException e) {
            LOG.error("error message", e);
        }
    }

    @Override
    public DataSource buildDataSource(String driverClass, String userName, String password, String url,
                                      Map<String, String> connectParam) throws LoadConfigException {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(driverClass);
        ds.setUsername(userName);
        ds.setPassword(password);
        ds.setUrl(url);

        // 设置连接池信息
        ds.setValidationQuery("SELECT 1");
        for (Map.Entry<String, String> entry : connectParam.entrySet()) {
            try {
                setConnectionParam(ds, entry.getKey(), entry.getValue());
            } catch (Exception e) {
                LOG.warn("无法识别的连接池参数:" + entry);
            }
        }

        return ds;
    }

}
