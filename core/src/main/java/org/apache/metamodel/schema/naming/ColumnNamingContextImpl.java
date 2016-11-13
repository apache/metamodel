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
package org.apache.metamodel.schema.naming;

import org.apache.metamodel.schema.Table;

public class ColumnNamingContextImpl implements ColumnNamingContext {

    private final int columnIndex;
    private final Table table;
    private final String intrinsicColumnName;

    /**
     * 
     * @param table
     * @param intrinsicColumnName
     * @param columnIndex
     */
    public ColumnNamingContextImpl(Table table, String intrinsicColumnName, int columnIndex) {
        this.table = table;
        this.intrinsicColumnName = intrinsicColumnName;
        this.columnIndex = columnIndex;
    }

    /**
     * 
     * @param columnIndex
     */
    public ColumnNamingContextImpl(int columnIndex) {
        this(null, null, columnIndex);
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
    public String getIntrinsicColumnName() {
        return intrinsicColumnName;
    }

}
