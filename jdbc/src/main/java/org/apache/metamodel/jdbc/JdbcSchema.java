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
package org.apache.metamodel.jdbc;

import java.io.ObjectStreamException;
import java.sql.Connection;

import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;

/**
 * Schema implementation for JDBC data contexts
 */
final class JdbcSchema extends MutableSchema {

    private static final long serialVersionUID = 7543633400859277467L;
    private transient MetadataLoader _metadataLoader;

    public JdbcSchema(String name, MetadataLoader metadataLoader) {
        super(name);
        _metadataLoader = metadataLoader;
    }

    protected void refreshTables(Connection connection) {
        if (_metadataLoader != null) {
            _metadataLoader.loadTables(this, connection);
        }
    }

    public void loadRelations(Connection connection) {
        if (_metadataLoader != null) {
            if (connection == null) {
                _metadataLoader.loadRelations(this);
            } else {
                _metadataLoader.loadRelations(this, connection);
            }
        }
    }
    
    public void loadRelations() {
        loadRelations(null);
    }

    public Schema toSerializableForm() {
        MutableTable[] tables = getTables();
        for (MutableTable table : tables) {
            table.getColumns();
            table.getIndexedColumns();
            table.getPrimaryKeys();
        }
        loadRelations();
        return this;
    }

    /**
     * Called by the Java Serialization API to serialize the object.
     */
    private Object writeReplace() throws ObjectStreamException {
        return toSerializableForm();
    }
}