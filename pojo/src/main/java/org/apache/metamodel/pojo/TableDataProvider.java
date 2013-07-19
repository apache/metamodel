/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.pojo;

import java.io.Serializable;
import java.util.Map;

import org.apache.metamodel.util.HasName;
import org.apache.metamodel.util.SimpleTableDef;

/**
 * Represents a named collection to be comprehended by a {@link PojoDataContext}
 * .
 */
public interface TableDataProvider<E> extends HasName, Iterable<E>, Serializable {

    public SimpleTableDef getTableDef();

    public Object getValue(String column, E record);

    public void insert(Map<String, Object> recordData);
}
