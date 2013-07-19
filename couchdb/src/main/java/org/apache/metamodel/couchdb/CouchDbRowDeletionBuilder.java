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

import java.util.List;

import org.ektorp.CouchDbConnector;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.delete.AbstractRowDeletionBuilder;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.schema.Table;

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
