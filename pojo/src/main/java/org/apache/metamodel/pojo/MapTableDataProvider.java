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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.metamodel.util.SimpleTableDef;

/**
 * {@link TableDataProvider} based on an {@link Collection} (for instance a
 * {@link List}) of {@link Map}s (for instance {@link HashMap}s).
 */
public class MapTableDataProvider implements TableDataProvider<Map<String, ? extends Object>> {

    private static final long serialVersionUID = 1L;
    private final SimpleTableDef _tableDef;
    private final Collection<Map<String, ?>> _maps;

    public MapTableDataProvider(SimpleTableDef tableDef, Collection<Map<String, ?>> maps) {
        _tableDef = tableDef;
        _maps = maps;
    }

    @Override
    public String getName() {
        return getTableDef().getName();
    }

    @Override
    public Iterator<Map<String, ? extends Object>> iterator() {
        return _maps.iterator();
    }

    @Override
    public SimpleTableDef getTableDef() {
        return _tableDef;
    }

    @Override
    public Object getValue(String column, Map<String, ? extends Object> record) {
        return record.get(column);
    }

    @Override
    public void insert(Map<String, Object> recordData) {
        _maps.add(recordData);
    }
}
