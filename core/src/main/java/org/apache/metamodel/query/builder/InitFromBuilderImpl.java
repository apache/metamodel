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
package org.apache.metamodel.query.builder;

import java.util.Arrays;
import java.util.List;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.BaseObject;

public final class InitFromBuilderImpl extends BaseObject implements InitFromBuilder {

    private DataContext dataContext;
    private Query query;

    public InitFromBuilderImpl(DataContext dataContext) {
        this.dataContext = dataContext;
        this.query = new Query();
    }

    @Override
    public TableFromBuilder from(Table table) {
        if (table == null) {
            throw new IllegalArgumentException("table cannot be null");
        }
        return new TableFromBuilderImpl(table, query, dataContext);
    }

    @Override
    public TableFromBuilder from(String schemaName, String tableName) {
        if (schemaName == null) {
            throw new IllegalArgumentException("schemaName cannot be null");
        }
        if (tableName == null) {
            throw new IllegalArgumentException("tableName cannot be null");
        }
        Schema schema = dataContext.getSchemaByName(schemaName);
        if (schema == null) {
            schema = dataContext.getDefaultSchema();
        }
        return from(schema, tableName);
    }

    @Override
    public TableFromBuilder from(Schema schema, String tableName) {
        Table table = schema.getTableByName(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Nu such table '" + tableName + "' found in schema: " + schema
                    + ". Available tables are: " + Arrays.toString(schema.getTableNames()));
        }
        return from(table);
    }

    @Override
    public TableFromBuilder from(String tableName) {
        if (tableName == null) {
            throw new IllegalArgumentException("tableName cannot be null");
        }
        Table table = dataContext.getTableByQualifiedLabel(tableName);
        if (table == null) {
            throw new IllegalArgumentException("No such table: " + tableName);
        }
        return from(table);
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(query);
    }
}