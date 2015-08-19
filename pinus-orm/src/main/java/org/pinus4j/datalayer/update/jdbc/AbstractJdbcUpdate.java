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

package org.pinus4j.datalayer.update.jdbc;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.pinus4j.api.SQL;
import org.pinus4j.datalayer.AbstractDataLayer;
import org.pinus4j.datalayer.SQLBuilder;
import org.pinus4j.datalayer.update.IDataUpdate;
import org.pinus4j.entity.meta.DBTablePK;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKValue;
import org.pinus4j.utils.BeansUtil;
import org.pinus4j.utils.JdbcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * 抽象的数据库更新操作组件.
 *
 * @author duanbn
 * @since 0.7.1
 */
public abstract class AbstractJdbcUpdate extends AbstractDataLayer implements IDataUpdate {

    public static final Logger   LOG               = LoggerFactory.getLogger(AbstractJdbcUpdate.class);

    /**
     * 执行保存数据操作.
     *
     * @param conn 数据库连接
     * @param entities 需要被保存的数据
     * @param tableIndex 分片表下标. 当-1时忽略下标
     * @throws SQLException
     */
    protected List<PKValue> _saveBatchWithAutoGeneratedKeys(Connection conn, List<? extends Object> entities,
                                                            int tableIndex) throws SQLException {
        List<PKValue> pks = Lists.newArrayList();

        PreparedStatement ps = null;

        SQL sql = null;
        Class<?> clazz = null;
        try {
            for (Object entity : entities) {

                sql = SQLBuilder.getInsert(entity, tableIndex);
                if (ps == null) {
                    ps = conn.prepareStatement(sql.getSql(), Statement.RETURN_GENERATED_KEYS);
                }
                fillParam(ps, sql);

                ps.executeUpdate();

                // 获取自增主键
                clazz = entity.getClass();
                if (!entityMetaManager.isUnionKey(clazz)) {
                    DBTablePK dbTablePK = entityMetaManager.getNotUnionPrimaryKey(clazz);
                    if (dbTablePK.isAutoIncrement()) {
                        ResultSet rs = ps.getGeneratedKeys();

                        Field f = BeansUtil.getField(clazz, dbTablePK.getField());
                        Object incrPK = null;
                        if (rs.next()) {
                            incrPK = rs.getObject(1);
                            if (f.getType() == Integer.TYPE || f.getType() == Integer.class) {
                                BeansUtil.setProperty(entity, dbTablePK.getField(), ((Long) incrPK).intValue());
                                pks.add(PKValue.valueOf(((Long) incrPK).intValue()));
                            } else {
                                BeansUtil.setProperty(entity, dbTablePK.getField(), incrPK);
                                pks.add(PKValue.valueOf(incrPK));
                            }
                        }
                    } else {
                        pks.add(PKValue.valueOf(BeansUtil.getProperty(entity, dbTablePK.getField())));
                    }
                }
            }
        } finally {
            JdbcUtil.close(ps);
        }

        return pks;
    }

    /**
     * 批量保存数据，忽略自增主键.
     * 
     * @param conn
     * @param entities
     * @param tableIndex
     * @throws SQLException
     */
    protected int _saveBatchWithoutAutoGeneratedKeys(Connection conn, List<? extends Object> entities, int tableIndex)
            throws SQLException {
        int insertCount = 0;
        PreparedStatement ps = null;
        try {

            SQL sql = null;
            for (Object entity : entities) {
                if (ps == null) {
                    sql = SQLBuilder.getInsert(entity, tableIndex);
                    ps = conn.prepareStatement(sql.getSql(), Statement.RETURN_GENERATED_KEYS);
                }

                fillParam(ps, sql);

                ps.addBatch();
            }

            int[] insertCountArray = ps.executeBatch();

            for (int i = 0; i < insertCountArray.length; i++) {
                insertCount += insertCountArray[i];
            }
        } finally {
            JdbcUtil.close(ps);
        }

        return insertCount;
    }

    /**
     * @param tableIndex 等于-1时会被忽略.
     * @throws SQLException
     */
    protected int _removeByPks(Connection conn, List<EntityPK> pks, Class<?> clazz, int tableIndex) throws SQLException {
        int removeCount = 0;

        PreparedStatement ps = null;
        try {

            SQL sql = SQLBuilder.buildDeleteByPks(clazz, tableIndex, pks);
            ps = conn.prepareStatement(sql.getSql());
            fillParam(ps, sql);

            removeCount = ps.executeUpdate();
        } finally {
            JdbcUtil.close(ps);
        }

        return removeCount;
    }

    /**
     * @param tableIndex 等于-1时会被忽略.
     * @throws SQLException
     */
    protected int _updateBatch(Connection conn, List<? extends Object> entities, int tableIndex) throws SQLException {
        int updateCount = 0;

        PreparedStatement ps = null;
        try {

            for (Object entity : entities) {
                SQL sql = SQLBuilder.getUpdate(entity, tableIndex);

                if (ps == null) {
                    ps = conn.prepareStatement(sql.getSql());
                }

                fillParam(ps, sql);
                ps.addBatch();
            }

            int[] updateCountArray = ps.executeBatch();
            for (int i = 0; i < updateCountArray.length; i++) {
                updateCount += updateCountArray[i];
            }
        } finally {
            JdbcUtil.close(ps);
        }

        return updateCount;
    }

}
