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
package org.apache.metamodel.drop;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

/**
 * Represents a single DROP TABLE operation to be applied to a
 * {@link UpdateableDataContext}. Instead of providing a custom implementation
 * of the {@link UpdateScript} interface, one can use this pre-built drop table
 * implementation. Some {@link DataContext}s may even optimize specifically
 * based on the knowledge that there will only be a single table dropped.
 */
public final class DropTable implements UpdateScript {

    private final String _schemaName;
    private final String _tableName;

    public DropTable(Table table) {
        this(table.getSchema().getName(), table.getName());
    }

    public DropTable(String tableName) {
        this((String) null, tableName);
    }

    public DropTable(Schema schema, String tableName) {
        this(schema.getName(), tableName);
    }

    public DropTable(String schemaName, String tableName) {
        _schemaName = schemaName;
        _tableName = tableName;
    }

    @Override
    public void run(UpdateCallback callback) {
        final TableDropBuilder dropBuilder;
        if (_schemaName == null) {
            dropBuilder = callback.dropTable(_tableName);
        } else {
            dropBuilder = callback.dropTable(_schemaName, _tableName);
        }
        dropBuilder.execute();
    }

}
