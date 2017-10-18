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
package org.apache.metamodel.schema;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Represents a virtual table that acts as an alias for another table.
 */
public class AliasTable extends AbstractTable implements WrappingTable {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final Schema schema;
    private final Table aliasedTable;

    public AliasTable(String name, Schema schema, Table aliasedTable) {
        this.name = name;
        this.schema = schema;
        this.aliasedTable = aliasedTable;
    }
    
    @Override
    public Table getWrappedTable() {
        return aliasedTable;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<Column> getColumns() {
        return aliasedTable.getColumns();
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public TableType getType() {
        return TableType.ALIAS;
    }

    @Override
    public Collection<Relationship> getRelationships() {
        return Collections.emptyList();
    }

    @Override
    public String getRemarks() {
        return null;
    }

    @Override
    public String getQuote() {
        return null;
    }

}
