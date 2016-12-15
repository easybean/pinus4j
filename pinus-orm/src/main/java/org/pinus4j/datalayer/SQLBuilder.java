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

package org.pinus4j.datalayer;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.pinus4j.api.SQL;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.impl.DefaultQueryImpl;
import org.pinus4j.api.query.impl.DefaultQueryImpl.OrderBy;
import org.pinus4j.constant.Const;
import org.pinus4j.entity.DefaultEntityMetaManager;
import org.pinus4j.entity.IEntityMetaManager;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKName;
import org.pinus4j.entity.meta.PKValue;
import org.pinus4j.utils.BeansUtil;
import org.pinus4j.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * SQL工具类.
 * 
 * @author duanbn
 */
public class SQLBuilder {

    public static final Logger               LOG               = LoggerFactory.getLogger(SQLBuilder.class);

    /**
     * select count语句缓存.
     */
    private static final Map<String, String> _selectCountCache = new ConcurrentHashMap<String, String>();

    private static IEntityMetaManager        entityMetaManager = DefaultEntityMetaManager.getInstance();

    /**
     * 拼装sql. SELECT pkName FROM tableName {IQuery.getSql()}
     * 
     * @return sql语句.
     */
    public static <T> SQL buildSelectPkByQuery(Class<T> clazz, int tableIndex, IQuery<T> query) {
        String tableName = entityMetaManager.getTableName(clazz, tableIndex);

        PKName[] pkNames = entityMetaManager.getPkName(clazz);
        StringBuilder pkField = new StringBuilder();
        for (PKName pkName : pkNames) {
            pkField.append('`').append(pkName.getValue()).append('`').append(',');
        }
        pkField.deleteCharAt(pkField.length() - 1);

        SQL querySQL = ((DefaultQueryImpl<T>) query).getWhereSql();

        StringBuilder sqlText = new StringBuilder("SELECT " + pkField.toString() + " FROM ");
        sqlText.append('`').append(tableName).append('`');
        String whereSql = querySQL.getSql();
        if (StringUtil.isNotBlank(whereSql))
            sqlText.append(whereSql);

        SQL sql = SQL.valueOf(sqlText.toString(), querySQL.getParams());

        debugSQL(sql.getSql());
        debugSQLParam(sql.getParams());

        return sql;
    }

    /**
     * 拼装sql. SELECT {fields} FROM tableName {IQuery.getSql()}
     * 
     * @return sql语句.
     */
    public static <T> SQL buildSelectByQuery(Class<T> clazz, int tableIndex, IQuery<T> query) {
        String tableName = entityMetaManager.getTableName(clazz, tableIndex);

        StringBuilder fields = new StringBuilder();
        if (((DefaultQueryImpl<T>) query).hasQueryFields()) {
            for (String field : ((DefaultQueryImpl<T>) query).getFields()) {
                fields.append('`').append(field).append('`').append(",");
            }
            fields.deleteCharAt(fields.length() - 1);
        } else {
            fields.append("*");
        }

        SQL querySQL = ((DefaultQueryImpl<T>) query).getWhereSql();

        StringBuilder sqlText = new StringBuilder("SELECT ");
        sqlText.append(fields.toString()).append(" FROM ");
        sqlText.append('`').append(tableName).append('`');
        String whereSql = querySQL.getSql();
        if (StringUtil.isNotBlank(whereSql))
            sqlText.append(((DefaultQueryImpl<T>) query).getWhereSql());

        SQL sql = SQL.valueOf(sqlText.toString(), querySQL.getParams());

        debugSQL(sql.getSql());
        debugSQLParam(sql.getParams());

        return sql;
    }

    public static <T> SQL buildSelectCountByQuery(Class<T> clazz, int tableIndex, IQuery<T> query) {
        String tableName = entityMetaManager.getTableName(clazz, tableIndex);

        StringBuilder sqlText = new StringBuilder("SELECT count(*) FROM ");
        sqlText.append('`').append(tableName).append('`');

        SQL querySQL = ((DefaultQueryImpl<T>) query).getWhereSql();

        String whereSql = querySQL.getSql();
        if (StringUtil.isNotBlank(whereSql))
            sqlText.append(((DefaultQueryImpl<T>) query).getWhereSql());

        SQL sql = SQL.valueOf(sqlText.toString(), querySQL.getParams());

        debugSQL(sql.getSql());
        debugSQLParam(sql.getParams());

        return sql;
    }

    public static PreparedStatement buildSelectBySqlGlobal(Connection conn, SQL sql) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sql.getSql());
        List<Object> params = sql.getParams();
        if (params != null) {
            for (int i = 1; i <= params.size(); i++) {
                ps.setObject(i, params.get(i - 1));
            }
        }

        debugSQL(sql.getSql());
        debugSQLParam(sql.getParams());

        return ps;
    }

    /**
     * 拼装sql. 根据SQL对象生成查询语句, 此sql语句不能包含limit
     * 
     * @param conn 数据库连接
     * @param sql 查询对象
     * @param tableIndex 分表下标
     * @return PreparedStatement
     * @throws SQLException
     */
    public static PreparedStatement buildSelectBySql(Connection conn, SQL sql, int tableIndex) throws SQLException {
        String s = SQLParser.addTableIndex(sql.getSql(), tableIndex);

        PreparedStatement ps = conn.prepareStatement(s);
        List<Object> params = sql.getParams();
        if (params != null) {
            for (int i = 1; i <= params.size(); i++) {
                ps.setObject(i, params.get(i - 1));
            }
        }

        debugSQL(sql.getSql());
        debugSQLParam(sql.getParams());

        return ps;
    }

    public static String buildSelectCountGlobalSql(Class<?> clazz) {
        String tableName = entityMetaManager.getTableName(clazz, -1);

        StringBuilder SQL = new StringBuilder("SELECT count(*) ").append("FROM ");
        SQL.append('`').append(tableName).append('`');

        debugSQL(SQL.toString());

        return SQL.toString();
    }

    /**
     * 拼装sql. SELECT count(*) FROM tableName
     * 
     * @param clazz 数据对象class
     * @param tableIndex 分表下标
     * @return SELECT count(*) FROM tableName
     */
    public static String buildSelectCountSql(Class<?> clazz, int tableIndex) {
        String sql = _selectCountCache.get(clazz.getName() + tableIndex);
        if (sql != null) {
            debugSQL(sql);
            return sql;
        }

        String tableName = entityMetaManager.getTableName(clazz, tableIndex);

        StringBuilder SQL = new StringBuilder("SELECT count(*) ").append("FROM ");
        SQL.append('`').append(tableName).append('`');

        _selectCountCache.put(clazz.getName() + tableIndex, SQL.toString());

        debugSQL(SQL.toString());

        return SQL.toString();
    }

    /**
     * 拼装select sql. SELECT field, field FROM tableName WHERE pk in (?, ?, ?)
     * 
     * @param clazz 数据对象
     * @param tableIndex 表下标
     * @param pks 主键
     * @return sql语句
     * @throws SQLException
     */
    public static SQL buildSelectByPks(EntityPK[] pks, List<OrderBy> orders, Class<?> clazz, int tableIndex)
            throws SQLException {
        Field[] fields = BeansUtil.getFields(clazz, true);
        String tableName = entityMetaManager.getTableName(clazz, tableIndex);

        StringBuilder whereSql = new StringBuilder();
        StringBuilder orderSql = new StringBuilder();
        StringBuilder findInSet = new StringBuilder();

        // build where
        // build find in set just only not union pk.
        List<Object> paramList = Lists.newArrayList();
        if (entityMetaManager.isUnionKey(clazz)) {
            // union pk, build (pk1 = ? and pk2 = ?) or (pk1 = ? and pk2 = ?)
            for (EntityPK pk : pks) {
                whereSql.append("(");
                for (int i = 0; i < pk.getPkNames().length; i++) {
                    whereSql.append('`').append(pk.getPkNames()[i].getValue()).append('`');
                    whereSql.append("=").append('?');
                    whereSql.append(" and ");

                    paramList.add(formatValue(pk.getPkValues()[i].getValue()));
                }
                whereSql.delete(whereSql.length() - 5, whereSql.length());
                whereSql.append(")");
                whereSql.append(" or ");
            }
            whereSql.delete(whereSql.length() - 4, whereSql.length());
        } else {
            // not union pk, build pk in (?,?,?)
            String pkName = entityMetaManager.getNotUnionPkName(clazz).getValue();
            findInSet.append("find_in_set(").append(pkName).append(",'");
            whereSql.append(pkName).append(" in (");
            for (EntityPK pk : pks) {
                whereSql.append("?,");
                findInSet.append(pk.getPkValues()[0].getValue()).append(',');
                paramList.add(formatValue(pk.getPkValues()[0].getValue()));
            }
            whereSql.deleteCharAt(whereSql.length() - 1);
            whereSql.append(")");
            findInSet.deleteCharAt(findInSet.length() - 1);
            findInSet.append("')");
        }

        // build order
        if (orders != null && !orders.isEmpty()) {
            orderSql.append(" order by ");
            for (OrderBy orderBy : orders) {
                orderSql.append('`').append(orderBy.getField()).append('`');
                orderSql.append(" ");
                orderSql.append(orderBy.getOrder().getValue());
                orderSql.append(",");
            }
            orderSql.deleteCharAt(orderSql.length() - 1);
        } else if (StringUtil.isNotBlank(findInSet.toString())) {
            orderSql.append(" order by ");
            orderSql.append(findInSet);
        }

        // build sql
        StringBuilder sqlText = new StringBuilder("SELECT ");
        for (Field field : fields) {
            sqlText.append('`').append(BeansUtil.getFieldName(field)).append('`').append(",");
        }
        sqlText.deleteCharAt(sqlText.length() - 1);
        sqlText.append(" FROM ").append('`').append(tableName).append('`');
        sqlText.append(" WHERE ").append(whereSql.toString());
        if (StringUtil.isNotBlank(orderSql.toString())) {
            sqlText.append(orderSql);
        }

        SQL sql = SQL.valueOf(sqlText.toString(), paramList);

        debugSQL(sql.getSql());
        debugSQLParam(sql.getParams());

        return sql;
    }

    /**
     * 拼装sql. DELETE FROM tableName WHERE pk in (...)
     * 
     * @return DELETE语句
     * @throws SQLException
     */
    public static SQL buildDeleteByPks(Class<?> clazz, int tableIndex, List<EntityPK> pks) throws SQLException {
        String tableName = entityMetaManager.getTableName(clazz, tableIndex);

        StringBuilder whereSql = new StringBuilder();
        List<Object> paramList = Lists.newArrayList();
        for (EntityPK pk : pks) {
            whereSql.append("(");
            for (int i = 0; i < pk.getPkNames().length; i++) {
                whereSql.append('`').append(pk.getPkNames()[i].getValue()).append('`');
                whereSql.append("=").append('?');
                whereSql.append(" and ");

                paramList.add(formatValue(pk.getPkValues()[i].getValue()));
            }
            whereSql.delete(whereSql.length() - 5, whereSql.length());
            whereSql.append(")");
            whereSql.append(" or ");
        }
        whereSql.delete(whereSql.length() - 4, whereSql.length());

        StringBuilder sqlText = new StringBuilder("DELETE FROM ").append('`').append(tableName).append('`');
        sqlText.append(" WHERE ").append(whereSql.toString());

        SQL sql = SQL.valueOf(sqlText.toString(), paramList);

        debugSQL(sql.getSql());
        debugSQLParam(sql.getParams());

        return sql;
    }

    /**
     * 获取update PreparedStatement.
     * 
     * @param conn 数据库连接
     * @param entities 数据对象
     * @param tableIndex 分表下标
     * @return PreparedStatement
     * @throws SQLException
     */
    public static SQL getUpdate(Object entity, int tableIndex) throws SQLException {
        // 获取表名.
        String tableName = entityMetaManager.getTableName(entity, tableIndex);

        // 批量添加
        Map<String, Object> entityProperty = null;
        try {
            entityProperty = BeansUtil.describe(entity, true);
        } catch (Exception e) {
            throw new SQLException("解析实体对象失败", e);
        }
        // 拼装主键条件
        EntityPK entityPk = entityMetaManager.getEntityPK(entity);
        StringBuilder pkWhereSql = new StringBuilder();
        List<Object> whereParam = Lists.newArrayList();
        for (int i = 0; i < entityPk.getPkNames().length; i++) {
            pkWhereSql.append('`').append(entityPk.getPkNames()[i].getValue()).append('`');
            pkWhereSql.append("=").append('?');
            pkWhereSql.append(" and ");

            whereParam.add(formatValue(entityPk.getPkValues()[i].getValue()));
        }
        pkWhereSql.delete(pkWhereSql.length() - 5, pkWhereSql.length());

        // 生成update语句.
        List<Object> paramList = Lists.newArrayList();
        Set<Map.Entry<String, Object>> propertyEntrySet = entityProperty.entrySet();
        StringBuilder sqlText = new StringBuilder("UPDATE `" + tableName + "` SET ");
        Object value = null;
        for (Map.Entry<String, Object> propertyEntry : propertyEntrySet) {
            value = propertyEntry.getValue();
            sqlText.append('`').append(propertyEntry.getKey()).append('`');
            sqlText.append("=").append("?");
            sqlText.append(",");

            paramList.add(formatValue(value));
        }
        sqlText.deleteCharAt(sqlText.length() - 1);
        sqlText.append(" WHERE ").append(pkWhereSql.toString());
        paramList.addAll(whereParam);

        SQL sql = SQL.valueOf(sqlText.toString(), paramList);

        debugSQL(sqlText.toString());
        debugSQLParam(paramList);

        return sql;
    }

    /**
     * 根据指定对象创建一个SQL语句.
     * 
     * @param conn 数据库连接引用
     * @param entity 数据对象
     * @param tableIndex 分表下标
     * @return SQL语句
     * @throws SQLException 操作失败
     */
    public static SQL getInsert(Object entity, int tableIndex) throws SQLException {
        // 获取表名.
        String tableName = entityMetaManager.getTableName(entity, tableIndex);

        // 批量添加
        Map<String, Object> entityProperty = null;
        try {
            // 获取需要被插入数据库的字段.
            entityProperty = BeansUtil.describe(entity, true);
        } catch (Exception e) {
            throw new SQLException("解析实体对象失败", e);
        }

        // 生成insert语句.
        Set<Map.Entry<String, Object>> propertyEntrySet = entityProperty.entrySet();

        List<Object> paramList = Lists.newArrayList();
        StringBuilder sqlText = new StringBuilder("INSERT INTO `" + tableName + "` (");
        StringBuilder var = new StringBuilder();
        for (Map.Entry<String, Object> propertyEntry : propertyEntrySet) {
            sqlText.append('`').append(propertyEntry.getKey()).append('`').append(",");
            var.append('?').append(',');

            paramList.add(formatValue(propertyEntry.getValue()));
        }
        sqlText.deleteCharAt(sqlText.length() - 1);
        sqlText.append(") VALUES (");
        sqlText.append(var.deleteCharAt(var.length() - 1).toString());
        sqlText.append(")");

        SQL sql = SQL.valueOf(sqlText.toString(), paramList);

        debugSQL(sqlText.toString());
        debugSQLParam(paramList);

        return sql;
    }

    /**
     * 给定数据库查询结果集创建数据对性.
     * 
     * @param rs 数据库查询结果集
     * @return 数据对象列表
     */
    public static List<Map<String, Object>> createResultObject(ResultSet rs) throws SQLException {
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

        ResultSetMetaData rsmd = rs.getMetaData();
        Map<String, Object> one = null;
        while (rs.next()) {
            try {
                one = Maps.newLinkedHashMap();
                String fieldName = null;
                Object value = null;
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    fieldName = rsmd.getColumnName(i);
                    value = rs.getObject(i);
                    one.put(fieldName, value);
                }
                list.add(one);
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }

        return list;
    }

    /**
     * 给定数据库查询结果集创建数据对性.
     * 
     * @param clazz 数据对象class
     * @param rs 数据库查询结果集
     * @return 数据对象列表
     */
    public static <T> List<T> createResultObject(Class<T> clazz, ResultSet rs) throws SQLException {
        List<T> list = new ArrayList<T>();

        ResultSetMetaData rsmd = rs.getMetaData();
        T one = null;
        while (rs.next()) {
            try {
                one = (T) clazz.newInstance();
                String fieldName = null;
                Field f = null;
                Object value = null;
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    fieldName = rsmd.getColumnName(i);
                    f = BeansUtil.getField(clazz, fieldName);
                    if (f == null) {
                        continue;
                    }
                    value = _getRsValue(rs, f, i);
                    BeansUtil.setProperty(one, fieldName, value);
                }
                list.add(one);
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }

        return list;
    }

    /**
     * 将数据转换为数据对象
     * 
     * @param clazz 数据对象
     * @param rs 结果集
     * @return {pkValue, Object}
     * @throws SQLException
     */
    public static <T> Map<EntityPK, T> createResultObjectAsMap(Class<T> clazz, ResultSet rs) throws SQLException {
        Map<EntityPK, T> map = Maps.newLinkedHashMap();

        ResultSetMetaData rsmd = rs.getMetaData();
        T one = null;
        String fieldName = null;
        PKName[] pkNames = entityMetaManager.getPkName(clazz);
        PKValue[] pkValues = null;

        Field f = null;
        Object value = null;
        while (rs.next()) {
            try {
                one = (T) clazz.newInstance();

                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    fieldName = rsmd.getColumnName(i);
                    f = BeansUtil.getField(clazz, fieldName);
                    if (f == null) {
                        continue;
                    }
                    value = _getRsValue(rs, f, i);
                    BeansUtil.setProperty(one, fieldName, value);
                }

                pkValues = new PKValue[pkNames.length];
                for (int i = 0; i < pkNames.length; i++) {
                    pkValues[i] = PKValue.valueOf(rs.getObject(pkNames[i].getValue()));
                }

                map.put(EntityPK.valueOf(pkNames, pkValues), one);
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }

        return map;
    }

    private static Object _getRsValue(ResultSet rs, Field f, int i) throws SQLException {
        Object value = rs.getObject(i);

        if (value != null) {
            if (f.getType() == Boolean.TYPE || f.getType() == Boolean.class) {
                value = rs.getString(i).equals(Const.TRUE) ? true : false;
            } else if (f.getType() == Byte.TYPE || f.getType() == Byte.class) {
                value = rs.getByte(i);
            } else if (f.getType() == Character.TYPE || f.getType() == Character.class) {
                String s = rs.getString(i);
                if (s.length() > 0)
                    value = rs.getString(i).charAt(0);
                else
                    value = new Character('\u0000');
            } else if (f.getType() == Short.TYPE || f.getType() == Short.class) {
                value = rs.getShort(i);
            } else if (f.getType() == Long.TYPE || f.getType() == Long.class) {
                value = rs.getLong(i);
            } else if (f.getType() == Integer.TYPE || f.getType() == Integer.class) {
                if (value instanceof Boolean) {
                    if ((Boolean) value) {
                        value = new Integer(1);
                    } else {
                        value = new Integer(0);
                    }
                }
            }
        }

        return value;
    }

    /**
     * 格式化数据库值. 过滤特殊字符
     */
    public static Object formatValue(Object value) {
        Object format = null;

        if (value instanceof Character && ((int) (Character) value) == 39) {
            format = "'\\" + (Character) value + "'";
        } else {
            format = value;
        }

        return format;
    }

    public static void debugSQL(String sql) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(sql);
        }
    }

    /**
     * 打印SQL日志.
     */
    public static void debugSQLParam(List<Object> params) {
        if (LOG.isDebugEnabled()) {
            if (params != null && !params.isEmpty()) {
                StringBuilder paramText = new StringBuilder();
                for (Object param : params) {
                    paramText.append(param).append(',');
                }
                paramText.deleteCharAt(paramText.length() - 1);
                LOG.debug(paramText.toString());
            }
        }
    }

}
