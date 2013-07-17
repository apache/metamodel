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
package org.eobjects.metamodel.couchdb;

import org.ektorp.CouchDbInstance;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.create.AbstractTableCreationBuilder;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

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
        }

        if (table.getColumnByName(CouchDbDataContext.FIELD_REV) == null) {
            table.addColumn(1, new MutableColumn(CouchDbDataContext.FIELD_REV, ColumnType.VARCHAR, table, 1, false));
        }
    }

}
