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
package org.apache.metamodel.create;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.TableType;

/**
 * Represents a single CREATE TABLE operation to be applied to a
 * {@link UpdateableDataContext}. Instead of providing a custom implementation
 * of the {@link UpdateScript} interface, one can use this pre-built create
 * table implementation. Some {@link DataContext}s may even optimize
 * specifically based on the knowledge that there will only be a single table
 * created.
 */
public final class CreateTable implements UpdateScript {

    private final MutableTable _table;

    public CreateTable(Schema schema, String tableName) {
        _table = new MutableTable(tableName, TableType.TABLE, schema);
    }

    public CreateTable(String schemaName, String tableName) {
        _table = new MutableTable(tableName, TableType.TABLE, new MutableSchema(schemaName));
    }

    /**
     * Adds a column to the current builder
     * 
     * @param name
     * @return
     */
    public CreateTableColumnBuilder withColumn(String name) {
        MutableColumn column = new MutableColumn(name);
        _table.addColumn(column);
        return new CreateTableColumnBuilder(this, column);
    }

    @Override
    public void run(UpdateCallback callback) {
        callback.createTable(_table.getSchema().getName(), _table.getName()).like(_table).execute();
    }
}
