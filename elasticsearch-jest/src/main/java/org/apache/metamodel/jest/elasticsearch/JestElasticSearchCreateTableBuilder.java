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
package org.apache.metamodel.jest.elasticsearch;

import com.google.common.base.Strings;
import io.searchbox.indices.mapping.PutMapping;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.create.AbstractTableCreationBuilder;
import org.apache.metamodel.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

final class JestElasticSearchCreateTableBuilder extends AbstractTableCreationBuilder<JestElasticSearchUpdateCallback> {

    private static final Logger logger = LoggerFactory.getLogger(JestElasticSearchCreateTableBuilder.class);

    public JestElasticSearchCreateTableBuilder(JestElasticSearchUpdateCallback updateCallback, Schema schema, String name) {
        super(updateCallback, schema, name);
    }

    @Override
    public Table execute() throws MetaModelException {

        final MutableTable table = getTable();

        if (table.getColumnByName(JestElasticSearchDataContext.FIELD_ID) == null) {
            final MutableColumn idColumn = new MutableColumn(JestElasticSearchDataContext.FIELD_ID, ColumnType.STRING)
                    .setTable(table).setPrimaryKey(true);
            table.addColumn(0, idColumn);
        }

        final JestElasticSearchDataContext dataContext = getUpdateCallback().getDataContext();
        final String indexName = dataContext.getIndexName();

        final List<Object> sourceProperties = new ArrayList<>();
        for (Column column : table.getColumns()) {
            // each column is defined as a property pair of the form: ("field1",
            // "type=string,store=true")
            final String columnName = column.getName();
            if (JestElasticSearchDataContext.FIELD_ID.equals(columnName)) {
                // do nothing - the ID is a client-side construct
                continue;
            }
            sourceProperties.add(columnName);

            String type = getType(column);
            if (type == null) {
                sourceProperties.add("store=true");
            } else {
                sourceProperties.add("type=" + type + ",store=true");
            }
        }

        final PutMapping putMapping = new PutMapping.Builder(indexName, table.getName(), sourceProperties).build();
        JestClientExecutor.execute(dataContext.getElasticSearchClient(), putMapping);

        final MutableSchema schema = (MutableSchema) getSchema();
        schema.addTable(table);
        return table;
    }

    /**
     * Determines the best fitting type. For reference of ElasticSearch types,
     * see
     * 
     * <pre>
     * http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/mapping-core-types.html
     * </pre>
     * 
     */
    private String getType(Column column) {
        String nativeType = column.getNativeType();
        if (!Strings.isNullOrEmpty(nativeType)) {
            return nativeType;
        }

        final ColumnType type = column.getType();
        if (type == null) {
            throw new IllegalStateException("No column type specified for '" + column.getName()
                    + "' - cannot build ElasticSearch mapping without type.");
        }

        if (type.isLiteral()) {
            return "string";
        } else if (type == ColumnType.FLOAT) {
            return "float";
        } else if (type == ColumnType.DOUBLE || type == ColumnType.NUMERIC || type == ColumnType.NUMBER) {
            return "double";
        } else if (type == ColumnType.SMALLINT) {
            return "short";
        } else if (type == ColumnType.TINYINT) {
            return "byte";
        } else if (type == ColumnType.INTEGER) {
            return "integer";
        } else if (type == ColumnType.DATE || type == ColumnType.TIMESTAMP) {
            return "date";
        } else if (type == ColumnType.BINARY || type == ColumnType.VARBINARY) {
            return "binary";
        } else if (type == ColumnType.BOOLEAN || type == ColumnType.BIT) {
            return "boolean";
        } else if (type == ColumnType.MAP) {
            return "object";
        }

        throw new UnsupportedOperationException("Unsupported column type '" + type.getName() + "' of column '"
                + column.getName() + "' - cannot translate to an ElasticSearch type.");
    }
}
