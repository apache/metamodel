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
package org.apache.metamodel.hbase;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.create.AbstractTableCreationBuilder;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;

public class HBaseCreateTableBuilder extends AbstractTableCreationBuilder<HBaseUpdateCallback> {

    private Set<String> columnFamilies;

    public HBaseCreateTableBuilder(HBaseUpdateCallback updateCallback, Schema schema, String name,
            HBaseColumn[] outputColumns) {
        super(updateCallback, schema, name);
        if (!(schema instanceof MutableSchema)) {
            throw new IllegalArgumentException("Not a valid schema: " + schema);
        }
        columnFamilies = new LinkedHashSet<String>();
        for (int i = 0; i < outputColumns.length; i++) {
            columnFamilies.add(outputColumns[i].getColumnFamily());
        }
    }

    @Override
    public Table execute() throws MetaModelException {
        final MutableTable table = getTable();
        final SimpleTableDef emptyTableDef = new SimpleTableDef(table.getName(), columnFamilies.toArray(
                new String[columnFamilies.size()]));

        final HBaseUpdateCallback updateCallback = (HBaseUpdateCallback) getUpdateCallback();

        try {
            final HBaseWriter HbaseWriter = new HBaseWriter(HBaseDataContext.createConfig(updateCallback
                    .getConfiguration()));
            HbaseWriter.createTable(table.getName(), columnFamilies);
        } catch (IOException e) {
            e.printStackTrace();
        }

        final MutableSchema schema = (MutableSchema) table.getSchema();
        schema.addTable(new HBaseTable(updateCallback.getDataContext(), emptyTableDef, schema,
                HBaseConfiguration.DEFAULT_ROW_KEY_TYPE));
        return schema.getTableByName(table.getName());
    }

}
