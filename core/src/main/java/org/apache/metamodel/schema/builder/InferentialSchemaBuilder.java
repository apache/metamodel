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

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.metamodel.data.Document;
import org.apache.metamodel.data.DocumentSource;
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
    public void offerSources(DocumentSourceProvider documentSourceProvider) {
        final DocumentSource documentSource = documentSourceProvider.getMixedDocumentSourceForSampling();
        try {
            while (true) {
                final Document document = documentSource.next();
                if (document == null) {
                    break;
                }
                final String tableName = determineTable(document);
                addObservation(tableName, document);
            }
        } finally {
            documentSource.close();
        }
    }

    protected void offerDocumentSource(DocumentSource documentSource) {
        try {
            while (true) {
                final Document document = documentSource.next();
                if (document == null) {
                    break;
                }
                final String tableName = determineTable(document);
                addObservation(tableName, document);
            }
        } finally {
            documentSource.close();
        }
    }

    @Override
    public String getSchemaName() {
        return _schemaName;
    }

    /**
     * Determines which table a particular document should be mapped to.
     * 
     * @param document
     * @return
     */
    protected abstract String determineTable(Document document);

    public void addObservation(String table, Document document) {
        final InferentialTableBuilder tableBuilder = getTableBuilder(table);
        tableBuilder.addObservation(document);
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
            final MutableTable table = buildTable(getTableBuilder(tableName));
            table.setSchema(schema);
            schema.addTable(table);
        }

        return schema;
    }

    protected MutableTable buildTable(InferentialTableBuilder tableBuilder) {
        return tableBuilder.buildTable();
    }
}
