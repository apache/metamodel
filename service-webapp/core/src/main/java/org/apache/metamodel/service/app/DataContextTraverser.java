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
package org.apache.metamodel.service.app;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.service.app.exceptions.NoSuchColumnException;
import org.apache.metamodel.service.app.exceptions.NoSuchSchemaException;
import org.apache.metamodel.service.app.exceptions.NoSuchTableException;

/**
 * Utility object responsible for traversing the schema/table/column structures
 * of a {@link DataContext} based on String identifiers and names.
 * 
 * This class will throw the appropriate exceptions if needed which is more
 * communicative than the usual NPEs that would otherwise be thrown.
 */
public class DataContextTraverser {

    private final DataContext dataContext;

    public DataContextTraverser(DataContext dataContext) {
        this.dataContext = dataContext;
    }

    public Schema getSchema(String schemaName) {
        final Schema schema = dataContext.getSchemaByName(schemaName);
        if (schema == null) {
            throw new NoSuchSchemaException(schemaName);
        }
        return schema;
    }

    public Table getTable(String schemaName, String tableName) {
        final Table table = getSchema(schemaName).getTableByName(tableName);
        if (table == null) {
            throw new NoSuchTableException(tableName);
        }
        return table;
    }

    public Column getColumn(String schemaName, String tableName, String columnName) {
        final Column column = getTable(schemaName, tableName).getColumnByName(columnName);
        if (column == null) {
            throw new NoSuchColumnException(columnName);
        }
        return column;
    }
}
