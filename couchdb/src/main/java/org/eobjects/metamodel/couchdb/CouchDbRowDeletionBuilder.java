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

import java.util.List;

import org.ektorp.CouchDbConnector;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.delete.AbstractRowDeletionBuilder;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.schema.Table;

final class CouchDbRowDeletionBuilder extends AbstractRowDeletionBuilder {

    private final CouchDbUpdateCallback _updateCallback;

    public CouchDbRowDeletionBuilder(CouchDbUpdateCallback updateCallback, Table table) {
        super(table);
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() throws MetaModelException {
        Table table = getTable();
        List<FilterItem> whereItems = getWhereItems();

        CouchDbConnector connector = _updateCallback.getConnector(table.getName());
        CouchDbDataContext dataContext = _updateCallback.getDataContext();

        DataSet dataSet = dataContext.query().from(table)
                .select(CouchDbDataContext.FIELD_ID, CouchDbDataContext.FIELD_REV).where(whereItems).execute();
        try {
            while (dataSet.next()) {
                Row row = dataSet.getRow();
                String id = (String) row.getValue(0);
                String revision = (String) row.getValue(1);
                connector.delete(id, revision);
            }
        } finally {
            dataSet.close();
        }
    }

}
