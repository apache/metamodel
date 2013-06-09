/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
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
