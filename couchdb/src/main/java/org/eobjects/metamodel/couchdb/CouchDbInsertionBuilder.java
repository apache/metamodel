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

import java.util.HashMap;
import java.util.Map;

import org.ektorp.CouchDbConnector;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.insert.AbstractRowInsertionBuilder;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;

final class CouchDbInsertionBuilder extends AbstractRowInsertionBuilder<CouchDbUpdateCallback> {

    public CouchDbInsertionBuilder(CouchDbUpdateCallback updateCallback, Table table) {
        super(updateCallback, table);
    }

    @Override
    public void execute() throws MetaModelException {
        Table table = getTable();
        String name = table.getName();

        Object[] values = getValues();
        Column[] columns = getColumns();
        Map<String, Object> map = new HashMap<String, Object>();
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            if (isSet(column)) {
                map.put(column.getName(), values[i]);
            }
        }

        CouchDbConnector connector = getUpdateCallback().getConnector(name);
        connector.addToBulkBuffer(map);
    }
}
