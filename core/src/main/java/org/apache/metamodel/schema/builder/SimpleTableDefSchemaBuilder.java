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

import org.apache.metamodel.convert.DocumentConverter;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;

/**
 * A {@link SchemaBuilder} that builds a schema according to instructions in the
 * form of {@link SimpleTableDef} objects.
 */
public class SimpleTableDefSchemaBuilder implements SchemaBuilder {

    private final String _schemaName;
    private final SimpleTableDef[] _simpleTableDefs;

    public SimpleTableDefSchemaBuilder(String schemaName, SimpleTableDef... simpleTableDefs) {
        _schemaName = schemaName;
        _simpleTableDefs = simpleTableDefs;
    }

    @Override
    public void offerSources(DocumentSourceProvider documentSource) {
        // do nothing
    }
    
    @Override
    public String getSchemaName() {
        return _schemaName;
    }

    @Override
    public MutableSchema build() {
        final MutableSchema schema = new MutableSchema(_schemaName);
        for (final SimpleTableDef simpleTableDef : _simpleTableDefs) {
            final MutableTable table = simpleTableDef.toTable();
            schema.addTable(table);
            table.setSchema(schema);
        }
        return schema;
    }

    @Override
    public DocumentConverter getDocumentConverter(Table table) {
        return new ColumnNameAsKeysRowConverter();
    }

}
