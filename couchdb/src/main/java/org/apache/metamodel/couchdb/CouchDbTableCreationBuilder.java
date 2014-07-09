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
package org.apache.metamodel.couchdb;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.create.AbstractTableCreationBuilder;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.ektorp.CouchDbInstance;

final class CouchDbTableCreationBuilder extends AbstractTableCreationBuilder<CouchDbUpdateCallback> {

    public CouchDbTableCreationBuilder(CouchDbUpdateCallback updateCallback, Schema schema, String name) {
        super(updateCallback, schema, name);
    }

    @Override
    public Table execute() throws MetaModelException {
        MutableTable table = getTable();

        String name = table.getName();

        CouchDbInstance instance = getUpdateCallback().getDataContext().getCouchDbInstance();
        instance.createDatabase(name);

        addMandatoryColumns(table);

        MutableSchema schema = (MutableSchema) table.getSchema();
        schema.addTable(table);

        return table;
    }

    /**
     * Verifies, and adds if nescesary, the two mandatory columns: _id and _rev.
     * 
     * @param table
     */
    public static void addMandatoryColumns(MutableTable table) {
        // add or correct ID column
        {
            MutableColumn idColumn = (MutableColumn) table.getColumnByName(CouchDbDataContext.FIELD_ID);
            if (idColumn == null) {
                idColumn = new MutableColumn(CouchDbDataContext.FIELD_ID, ColumnType.VARCHAR, table, 0, false);
                table.addColumn(0, idColumn);
            }
            idColumn.setPrimaryKey(true);
            idColumn.setNullable(false);
        }

        // add or correct _rev column
        {
            MutableColumn revColumn = (MutableColumn) table.getColumnByName(CouchDbDataContext.FIELD_REV);
            if (revColumn == null) {
                revColumn = new MutableColumn(CouchDbDataContext.FIELD_REV, ColumnType.VARCHAR, table, 1, false);
                table.addColumn(1, revColumn);
            }
            revColumn.setNullable(false);
        }
    }

}
