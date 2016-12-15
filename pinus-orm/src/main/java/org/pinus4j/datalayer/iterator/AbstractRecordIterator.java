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

package org.pinus4j.datalayer.iterator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.impl.DefaultQueryImpl;
import org.pinus4j.datalayer.IRecordIterator;
import org.pinus4j.datalayer.query.jdbc.AbstractJdbcQuery;
import org.pinus4j.entity.DefaultEntityMetaManager;
import org.pinus4j.entity.IEntityMetaManager;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.utils.BeansUtil;

/**
 * 抽象数据库记录迭代器.
 * 
 * @author duanbn
 */
public abstract class AbstractRecordIterator<E> extends AbstractJdbcQuery implements IRecordIterator<E> {
    
    private IEntityMetaManager entityMetaManager = DefaultEntityMetaManager.getInstance();

    public static final int STEP     = 5000;

    protected Class<E>      clazz;

    protected String        pkName;

    protected IQuery        query;

    protected Queue<E>      recordQ;
    protected int           step     = STEP;
    protected long          latestId = 0;
    protected long          maxId;

    public AbstractRecordIterator(Class<E> clazz) {
        // check pk type
        pkName = entityMetaManager.getNotUnionPkName(clazz).getValue();
        Class<?> type;
        try {
            type = BeansUtil.getField(clazz, pkName).getType();
        } catch (SecurityException e) {
            throw new DBOperationException("遍历数据失败, clazz " + clazz, e);
        }
        if (type != Long.TYPE && type != Integer.TYPE && type != Short.TYPE && type != Long.class && type != Long.class
                && type != Short.class) {
            throw new DBOperationException("被遍历的数据主键不是数值型");
        }

        this.clazz = clazz;

        if (this.query == null) {
            this.query = new DefaultQueryImpl();
        }

        this.recordQ = new LinkedList<E>();
    }

    @Override
    public E next() {
        return this.recordQ.poll();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("this iterator cann't doing remove");
    }

    @Override
    public List<E> nextMore() {
        List<E> data = new ArrayList<E>(this.recordQ);
        this.recordQ.clear();
        return data;
    }

    @Override
    public void setQuery(IQuery query) {
        if (query != null)
            this.query = query;
    }

    public abstract long getMaxId();

    public int getStep() {
        return step;
    }

    @Override
    public void setStep(int step) {
        this.step = step;
    }

}
