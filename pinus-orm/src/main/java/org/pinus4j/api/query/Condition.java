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

package org.pinus4j.api.query;

import java.lang.reflect.Array;

import org.pinus4j.datalayer.SQLBuilder;
import org.pinus4j.utils.ReflectUtil;
import org.pinus4j.utils.StringUtils;

/**
 * 查询条件.
 * 
 * @author duanbn
 */
public class Condition {

	/**
	 * 条件字段.
	 */
	private String field;
	/**
	 * 条件值.
	 */
	private Object value;
	/**
	 * 条件枚举.
	 */
	private QueryOpt opt;

	/**
	 * 保存or查询.
	 */
	private Condition[] orCond;

	/**
	 * 构造方法. 防止调用者直接创建此对象.
	 */
	private Condition() {
	}

	private Condition(Condition... conds) {
		this.orCond = conds;
	}

	/**
	 * 构造方法.
	 * 
	 * @param field
	 *            条件字段
	 * @param value
	 *            条件值
	 * @param opt
	 *            条件枚举
	 */
	private Condition(String field, Object value, QueryOpt opt,Class<?> clazz) {
		if (StringUtils.isBlank(field)) {
			throw new IllegalArgumentException("条件字段不能为空, condition field=" + field);
		}
		if (value == null) {
			throw new IllegalArgumentException("参数错误, condition value=" + value);
		}
		if (value.getClass().isArray() && Array.getLength(value) == 0) {
			throw new IllegalArgumentException("参数错误, condition value是数组并且数组长度为0");
		}

		this.field = ReflectUtil.getFieldName(ReflectUtil.getField(clazz, field));
		this.value = SQLBuilder.formatValue(value);
		this.opt = opt;
	}

	/**
	 * 构造方法.
	 *
	 * @param field
	 *            条件字段
	 * @param value
	 *            条件值
	 * @param opt
	 *            条件枚举
	 */
	private Condition(String field, Object value, QueryOpt opt) {
		if (StringUtils.isBlank(field)) {
			throw new IllegalArgumentException("条件字段不能为空, condition field=" + field);
		}
		if (value == null) {
			throw new IllegalArgumentException("参数错误, condition value=" + value);
		}
		if (value.getClass().isArray() && Array.getLength(value) == 0) {
			throw new IllegalArgumentException("参数错误, condition value是数组并且数组长度为0");
		}

		this.field = field;
		this.value = SQLBuilder.formatValue(value);
		this.opt = opt;
	}

	/**
	 * 返回当前条件对象表示的sql语句.
	 * 
	 * @return sql语句
	 */
	public String getSql() {
		StringBuilder SQL = new StringBuilder();
		if (orCond != null && orCond.length > 0) {
			SQL.append("(");
			for (Condition cond : orCond) {
				SQL.append(cond.getSql()).append(" OR ");
			}
			SQL.delete(SQL.lastIndexOf(" OR "), SQL.length());
			SQL.append(")");
			return SQL.toString();
		} else {
			SQL.append(field).append(" ").append(opt.getSymbol()).append(" ");
			switch (opt) {
			case IN:
				SQL.append("(");
				for (int i = 0; i < Array.getLength(this.value); i++) {
					Object val = Array.get(this.value, i);
					Class<?> clazz = val.getClass();
					if (clazz == String.class) {
						SQL.append("'").append(val).append("'");
					} else if (clazz == Boolean.class || clazz == Boolean.TYPE) {
						if ((Boolean) val) {
							SQL.append("'").append("1").append("'");
						} else {
							SQL.append("'").append("0").append("'");
						}
					} else {
						SQL.append(val);
					}
					SQL.append(",");
				}
				SQL.deleteCharAt(SQL.length() - 1);
				SQL.append(")");

				break;
			default:
				Object value = this.value;
				if (value instanceof String) {
					SQL.append(value);
				} else if (value instanceof Boolean) {
					if ((Boolean) value) {
						SQL.append("'").append("1").append("'");
					} else {
						SQL.append("'").append("0").append("'");
					}
				} else {
					SQL.append(value);
				}
				break;
			}
			return SQL.toString();
		}
	}

	@Override
	public String toString() {
		return getSql();
	}

	/**
	 * 等于条件.
	 * 
	 * @param field
	 *            条件字段
	 * @param value
	 *            字段值
	 */
	public static Condition eq(String field, Object value,Class<?> clazz) {
		if (value == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, value, QueryOpt.EQ,clazz);
		return cond;
	}

	/**
	 * 不等于条件.
	 * 
	 * @param field
	 *            条件字段
	 * @param value
	 *            字段值
	 */
	public static Condition noteq(String field, Object value,Class<?> clazz) {
		if (value == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, value, QueryOpt.NOTEQ,clazz);
		return cond;
	}

	/**
	 * 大于条件.
	 * 
	 * @param field
	 *            条件字段
	 * @param value
	 *            字段值
	 */
	public static Condition gt(String field, Object value,Class<?> clazz) {
		if (value == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, value, QueryOpt.GT, clazz);
		return cond;
	}

	/**
	 * 大于等于条件.
	 * 
	 * @param field
	 *            条件字段
	 * @param value
	 *            字段值
	 */
	public static Condition gte(String field, Object value,Class<?> clazz) {
		if (value == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, value, QueryOpt.GTE,clazz);
		return cond;
	}

	/**
	 * 小于条件.
	 * 
	 * @param field
	 *            条件字段
	 * @param value
	 *            字段值
	 */
	public static Condition lt(String field, Object value,Class<?> clazz) {
		if (value == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, value, QueryOpt.LT,clazz);
		return cond;
	}

	/**
	 * 小于等于条件.
	 * 
	 * @param field
	 *            条件字段
	 * @param value
	 *            字段值
	 */
	public static Condition lte(String field, Object value,Class<?> clazz) {
		if (value == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, value, QueryOpt.LTE,clazz);
		return cond;
	}

	/**
	 * in操作.
	 * 
	 * @param field
	 *            条件字段
	 * @param values
	 *            字段值
	 * 
	 * @return 当前条件对象
	 */
	public static Condition in(String field,Class<?> clazz, Object... values) {
		if (values == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, values, QueryOpt.IN, clazz);
		return cond;
	}

	public static Condition in(String field, byte[] values,Class<?> clazz) {
		if (values == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, values, QueryOpt.IN,clazz);
		return cond;
	}

	public static Condition in(String field, int[] values,Class<?> clazz) {
		if (values == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, values, QueryOpt.IN, clazz);
		return cond;
	}

	public static Condition in(String field, short[] values,Class<?> clazz) {
		if (values == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, values, QueryOpt.IN,clazz);
		return cond;
	}

	public static Condition in(String field, long[] values,Class<?> clazz) {
		if (values == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, values, QueryOpt.IN, clazz);
		return cond;
	}

	public static Condition in(String field, float[] values,Class<?> clazz) {
		if (values == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, values, QueryOpt.IN,clazz);
		return cond;
	}

	public static Condition in(String field, double[] values,Class<?> clazz) {
		if (values == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, values, QueryOpt.IN,clazz);
		return cond;
	}

	public static Condition in(String field, boolean[] values,Class<?> clazz) {
		if (values == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, values, QueryOpt.IN,clazz);
		return cond;
	}

	/**
	 * like查询.
	 * 
	 * @param field
	 *            条件字段
	 * @param value
	 *            字段值
	 */
	public static Condition like(String field, String value,Class<?> clazz) {
		if (value == null) {
			throw new IllegalArgumentException("参数错误, condition value=null");
		}
		Condition cond = new Condition(field, value, QueryOpt.LIKE,clazz);
		return cond;
	}

	/**
	 * 或查询.
	 * 
	 * @param conds
	 *            查询条件
	 */
	public static Condition or(Condition... conds) {
		if (conds == null || conds.length < 2) {
			throw new IllegalArgumentException("参数错误, or查询条件最少为2个");
		}
		Condition cond = new Condition(conds);
		return cond;
	}

}
