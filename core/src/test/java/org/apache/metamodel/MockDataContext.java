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
package org.apache.metamodel;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.metamodel.data.CachingDataSetHeader;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.EmptyDataSet;
import org.apache.metamodel.data.InMemoryDataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class MockDataContext extends QueryPostprocessDataContext {

    private final String _schemaName;
    private final String _tableName;
    private final String _value;

    public MockDataContext(String schemaName, String tableName, String value) {
        _schemaName = schemaName;
        _tableName = tableName;
        _value = value;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {

        final MutableSchema schema = new MutableSchema(_schemaName);
        final MutableTable primaryTable = new MutableTable(_tableName).setSchema(schema);
        primaryTable.addColumn(new MutableColumn("foo").setColumnNumber(0).setType(ColumnType.VARCHAR)
                .setTable(primaryTable));
        primaryTable.addColumn(new MutableColumn("bar").setColumnNumber(1).setType(ColumnType.VARCHAR)
                .setTable(primaryTable));
        primaryTable.addColumn(new MutableColumn("baz").setColumnNumber(2).setType(ColumnType.VARCHAR)
                .setTable(primaryTable));

        final MutableTable emptyTable = new MutableTable("an_empty_table").setSchema(schema);
        emptyTable.addColumn(new MutableColumn("foo").setColumnNumber(0).setType(ColumnType.VARCHAR)
                .setTable(emptyTable));
        emptyTable.addColumn(new MutableColumn("bar").setColumnNumber(1).setType(ColumnType.VARCHAR)
                .setTable(emptyTable));

        schema.addTable(primaryTable);
        schema.addTable(emptyTable);

        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return _schemaName;
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
        if (_tableName.equals(table.getName())) {
            final List<SelectItem> allSelectItems = table.getColumns().stream().map(SelectItem::new).collect(Collectors.toList());
            final DataSetHeader header = new CachingDataSetHeader(allSelectItems);
            final List<Row> data = new ArrayList<Row>();
            data.add(new DefaultRow(header, new Object[] { "1", "hello", "world" }, null));
            data.add(new DefaultRow(header, new Object[] { "2", _value, "world" }, null));
            data.add(new DefaultRow(header, new Object[] { "3", "hi", _value }, null));
            data.add(new DefaultRow(header, new Object[] { "4", "yo", "world" }, null));

            final DataSet sourceDataSet = new InMemoryDataSet(header, data);

            final List<SelectItem> columnSelectItems = columns.stream().map(SelectItem::new).collect(Collectors.toList());
            final DataSet selectionDataSet = MetaModelHelper.getSelection(columnSelectItems, sourceDataSet);
            return selectionDataSet;
        } else if ("an_empty_table".equals(table.getName())) {
            return new EmptyDataSet(columns.stream().map(SelectItem::new).collect(Collectors.toList()));
        }
        throw new UnsupportedOperationException();
    }

}
