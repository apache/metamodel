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
import org.apache.metamodel.delete.AbstractRowDeletionBuilder;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.schema.Table;

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
