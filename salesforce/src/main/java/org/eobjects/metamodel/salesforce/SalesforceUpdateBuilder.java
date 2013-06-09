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
package org.eobjects.metamodel.salesforce;

import java.util.ArrayList;
import java.util.List;

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.update.AbstractRowUpdationBuilder;

import com.sforce.soap.partner.sobject.SObject;

/**
 * Row updation builder for Salesforce
 */
public class SalesforceUpdateBuilder extends AbstractRowUpdationBuilder {

    private final SalesforceUpdateCallback _updateCallback;

    public SalesforceUpdateBuilder(SalesforceUpdateCallback updateCallback, Table table) {
        super(table);
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() throws MetaModelException {
        final List<String> idList = new ArrayList<String>();
        final List<FilterItem> whereItems = getWhereItems();

        for (FilterItem whereItem : whereItems) {
            _updateCallback.buildIdList(idList, whereItem);
        }

        final SObject[] updatedObjects = new SObject[idList.size()];
        for (int i = 0; i < updatedObjects.length; i++) {
            final SObject object = buildUpdatedObject(idList.get(i));
            updatedObjects[i] = object;
        }

        _updateCallback.update(updatedObjects);
    }

    private SObject buildUpdatedObject(String id) {
        final SObject obj = new SObject();
        obj.setId(id);
        obj.setType(getTable().getName());

        final Object[] values = getValues();
        final Column[] columns = getColumns();
        final boolean[] explicitNulls = getExplicitNulls();
        final List<String> nullFields = new ArrayList<String>();

        for (int i = 0; i < columns.length; i++) {
            final Object value = values[i];
            final Column column = columns[i];
            if (value == null) {
                if (explicitNulls[i]) {
                    nullFields.add(column.getName());
                }
            } else {
                obj.setField(column.getName(), value);
            }
        }
        obj.setFieldsToNull(nullFields.toArray(new String[nullFields.size()]));

        return obj;
    }

}
