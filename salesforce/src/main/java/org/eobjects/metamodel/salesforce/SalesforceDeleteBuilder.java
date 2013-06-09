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
import org.eobjects.metamodel.delete.AbstractRowDeletionBuilder;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.schema.Table;

/**
 * Row deletion builder for Salesforce.com
 */
final class SalesforceDeleteBuilder extends AbstractRowDeletionBuilder {

    private final SalesforceUpdateCallback _updateCallback;

    public SalesforceDeleteBuilder(SalesforceUpdateCallback updateCallback, Table table) {
        super(table);
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() throws MetaModelException {
        List<FilterItem> whereItems = getWhereItems();
        if (whereItems.isEmpty()) {
            throw new IllegalStateException(
                    "Salesforce only allows deletion of records by their specific IDs. Violated by not having any where items");
        }

        List<String> idList = new ArrayList<String>();
        for (FilterItem whereItem : whereItems) {
            _updateCallback.buildIdList(idList, whereItem);
        }

        _updateCallback.delete(idList.toArray(new String[idList.size()]));
    }

}
