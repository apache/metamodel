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

import org.apache.metamodel.convert.DocumentConverter;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Document;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.Resource;
import org.apache.metamodel.util.ResourceUtils;

/**
 * A very simple {@link SchemaBuilder} that builds a schema consisting of 1
 * table with 1 column, of type {@link Map}.
 */
public class SingleMapColumnSchemaBuilder implements SchemaBuilder, DocumentConverter {

    private final String _schemaName;
    private final String _tableName;
    private final String _columnName;

    public SingleMapColumnSchemaBuilder(Resource resource, String columnName) {
        this(ResourceUtils.getParentName(resource), resource.getName(), columnName);
    }

    public SingleMapColumnSchemaBuilder(String schemaName, String tableName, String columnName) {
        _schemaName = schemaName;
        _tableName = tableName;
        _columnName = columnName;
    }

    @Override
    public void offerSources(DocumentSourceProvider documentSourceProvider) {
        // do nothing
    }
    
    @Override
    public String getSchemaName() {
        return _schemaName;
    }

    @Override
    public MutableSchema build() {
        MutableSchema schema = new MutableSchema(_schemaName);
        MutableTable table = new MutableTable(_tableName, schema);
        table.addColumn(new MutableColumn(_columnName, ColumnType.MAP, table, 1, null, null, false, null, false, null));
        schema.addTable(table);
        return schema;
    }

    @Override
    public DocumentConverter getDocumentConverter(Table table) {
        return this;
    }

    @Override
    public Row convert(Document document, DataSetHeader header) {
        assert header.size() == 1;
        Object[] values = new Object[1];
        values[0] = document.getValues();
        return new DefaultRow(header, values);
    }

}
