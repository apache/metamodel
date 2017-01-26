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
package org.apache.metamodel.dynamodb;

import org.apache.metamodel.AbstractUpdateCallback;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.delete.RowDeletionBuilder;
import org.apache.metamodel.drop.TableDropBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

final class DynamoDbUpdateCallback extends AbstractUpdateCallback {
    
    private boolean interrupted = false;

    public DynamoDbUpdateCallback(DynamoDbDataContext dataContext) {
        super(dataContext);
    }
    
    public boolean isInterrupted() {
        return interrupted;
    }
    
    public void setInterrupted(boolean interrupted) {
        this.interrupted = interrupted;
    }

    @Override
    public DynamoDbDataContext getDataContext() {
        return (DynamoDbDataContext) super.getDataContext();
    }

    @Override
    public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
            IllegalStateException {
        return new DynamoDbTableCreationBuilder(this, schema, name);
    }

    @Override
    public boolean isDropTableSupported() {
        return true;
    }

    @Override
    public TableDropBuilder dropTable(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new DynamoDbTableDropBuilder(table, getDataContext());
    }

    @Override
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new DynamoDbRowInsertionBuilder(this, table);
    }

    @Override
    public boolean isDeleteSupported() {
        return false;
    }

    @Override
    public RowDeletionBuilder deleteFrom(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        // This could be implemented ...
        throw new UnsupportedOperationException();
    }

}
