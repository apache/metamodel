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

import java.util.ArrayList;
import java.util.List;

/**
 * A special purpose {@link Schema} wrapper which exposes an {@link AliasTable} "default_table" for convenience when the
 * table count is 1.
 */
public class DefaultTableAliasedSchema extends AbstractSchema {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_TABLE_NAME = "default_table";

    public static Schema wrapIfAppropriate(Schema schema) {
        List<Table> tables = schema.getTables();

        // ensure table size is 1
        if (tables.size() != 1) {
            return schema;
        }

        // ensure no name clashes
        if (DEFAULT_TABLE_NAME.equals(tables.get(0).getName())) {
            return schema;
        }

        if (schema instanceof MutableSchema) {
            final MutableSchema mutableSchema = (MutableSchema) schema;
            mutableSchema.addTable(createTable(schema, schema.getTable(0)));
            return schema;
        } else {
            return new DefaultTableAliasedSchema(schema);
        }
    }

    private static AliasTable createTable(Schema schema, Table delegateTable) {
        return new AliasTable(DEFAULT_TABLE_NAME, schema, delegateTable);
    }

    private final Schema delegateSchema;

    private DefaultTableAliasedSchema(Schema delegateSchema) {
        this.delegateSchema = delegateSchema;
    }

    public Schema getDelegateSchema() {
        return delegateSchema;
    }

    @Override
    public String getName() {
        return delegateSchema.getName();
    }

    @Override
    public List<Table> getTables() {
        List<Table> tables = delegateSchema.getTables();
        // ensure mutability
        if (!(tables instanceof ArrayList)) {
            tables = new ArrayList<>(tables);
        }

        tables.add(createTable(this, tables.get(0)));
        return tables;
    }

    @Override
    public String getQuote() {
        return delegateSchema.getQuote();
    }
}
