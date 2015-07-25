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

package org.pinus4j.entity.meta;

import java.io.Serializable;

/**
 * 数据库表的列的bean. 表示一个数据库表的一个列.
 * 
 * @author duanbn
 */
public class DBTablePK implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * 字段名
     */
    private String            field;

    /**
     * 字段类型
     */
    private String            type;

    /**
     * 字段长度
     */
    private int               length;

    /**
     * 注释
     */
    private String            comment;

    private boolean           isAutoIncrement;

    public PKName getPKName() {
        return PKName.valueOf(field);
    }

    public DBTableColumn toTableColumn() {
        DBTableColumn column = new DBTableColumn();
        column.setCanNull(false);
        column.setComment(this.comment);
        column.setDefaultValue(null);
        column.setField(this.field);
        column.setHasDefault(false);
        column.setLength(this.length);
        column.setType(this.type);
        column.setAutoIncrement(isAutoIncrement);
        return column;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public boolean isAutoIncrement() {
        return isAutoIncrement;
    }

    public void setAutoIncrement(boolean isAutoIncrement) {
        this.isAutoIncrement = isAutoIncrement;
    }

    @Override
    public String toString() {
        return "DBTablePrimaryKey [field=" + field + ", type=" + type + ", length=" + length + ", comment=" + comment
                + ", isAutoIncrement=" + isAutoIncrement + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((comment == null) ? 0 : comment.hashCode());
        result = prime * result + ((field == null) ? 0 : field.hashCode());
        result = prime * result + (isAutoIncrement ? 1231 : 1237);
        result = prime * result + length;
        result = prime * result + ((type == null) ? 0 : type.hashCode());
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
        DBTablePK other = (DBTablePK) obj;
        if (comment == null) {
            if (other.comment != null)
                return false;
        } else if (!comment.equals(other.comment))
            return false;
        if (field == null) {
            if (other.field != null)
                return false;
        } else if (!field.equals(other.field))
            return false;
        if (isAutoIncrement != other.isAutoIncrement)
            return false;
        if (length != other.length)
            return false;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        return true;
    }

}
