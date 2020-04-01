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
package org.apache.metamodel.schema.typing;

import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;


/**
 * An implementation of {@link ColumnTypingContext} that holds necessary context about the column being configured.
 */
public class ColumnTypingContextImpl implements ColumnTypingContext {

    private final int columnIndex;

    private final Table table;

    private final ColumnType intrinsicColumnType;


    /**
     * Creates a context to conifgure a column for a specific table.
     * @param table               The table that contains the column
     * @param intrinsicColumnType The column type that represents the type configured in the datastore
     * @param columnIndex         the index in the table of the column being configured.
     */
    public ColumnTypingContextImpl( Table table, ColumnType intrinsicColumnType, int columnIndex ) {
        this.table = table;
        this.intrinsicColumnType = intrinsicColumnType;
        this.columnIndex = columnIndex;
    }


    /**
     * Creates a context a column to be configured.
     * @param columnIndex the index in the table of the column being configured.
     */
    public ColumnTypingContextImpl( int columnIndex ) {
        this( null, null, columnIndex );
    }


    @Override
    public int getColumnIndex() {
        return columnIndex;
    }


    @Override
    public Table getTable() {
        return table;
    }


    @Override
    public ColumnType getIntrinsicColumnType() {
        return intrinsicColumnType;
    }

}
