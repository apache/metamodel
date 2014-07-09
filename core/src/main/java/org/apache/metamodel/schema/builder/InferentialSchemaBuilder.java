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
package org.apache.metamodel.schema.builder;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;

public abstract class InferentialSchemaBuilder implements SchemaBuilder {

    private final ConcurrentMap<String, InferentialTableBuilder> _tableBuilders;
    private final String _schemaName;

    public InferentialSchemaBuilder(String schemaName) {
        _schemaName = schemaName;
        _tableBuilders = new ConcurrentHashMap<String, InferentialTableBuilder>();
    }

    @Override
    public void offerSource(DocumentSource documentSource) {
        while (true) {
            Map<String, ?> map = documentSource.next();
            if (map == null) {
                return;
            }
            String tableName = determineTable(map);
            addObservation(tableName, map);
        }
    }

    /**
     * Determines which table a particular document should be mapped to.
     * 
     * @param map
     * @return
     */
    protected abstract String determineTable(Map<String, ?> map);

    public void addObservation(String table, Map<String, ?> values) {
        final InferentialTableBuilder tableBuilder = getTableBuilder(table);
        tableBuilder.addObservation(values);
    }

    public InferentialTableBuilder getTableBuilder(String table) {
        InferentialTableBuilder tableBuilder = _tableBuilders.get(table);
        if (tableBuilder == null) {
            tableBuilder = new InferentialTableBuilder(table);
            InferentialTableBuilder existingTableBuilder = _tableBuilders.putIfAbsent(table, tableBuilder);
            if (existingTableBuilder != null) {
                tableBuilder = existingTableBuilder;
            }
        }
        return tableBuilder;
    }

    @Override
    public MutableSchema build() {
        final MutableSchema schema = new MutableSchema(_schemaName);

        // Sort table names by moving them to a treeset
        final Set<String> tableNames = new TreeSet<String>(_tableBuilders.keySet());

        for (final String tableName : tableNames) {
            final MutableTable table = getTableBuilder(tableName).buildTable();
            table.setSchema(schema);
            schema.addTable(table);
        }

        return schema;
    }
}
