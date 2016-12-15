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

package org.pinus4j.api.query.impl;

/**
 * 数据库排序枚举.
 *
 * @author duanbn
 */
public enum Order {

    /**
     * 升序.
     */
    ASC("asc"),
    /**
     * 降序.
     */
    DESC("desc");

    private String value;

    private Order(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

}
