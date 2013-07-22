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
package org.apache.metamodel.salesforce;

import java.util.ArrayList;
import java.util.List;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.AbstractRowUpdationBuilder;

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
