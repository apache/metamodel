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
import java.util.Iterator;
import java.util.List;

import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.data.CachingDataSetHeader;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.EmptyDataSet;
import org.apache.metamodel.data.InMemoryDataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.delete.AbstractRowDeletionBuilder;
import org.apache.metamodel.delete.RowDeletionBuilder;
import org.apache.metamodel.drop.TableDropBuilder;
import org.apache.metamodel.insert.AbstractRowInsertionBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class MockUpdateableDataContext extends QueryPostprocessDataContext implements UpdateableDataContext {

    private final List<Object[]> _values = new ArrayList<Object[]>();

    private final MutableTable _table;
    private final MutableSchema _schema;

    public MockUpdateableDataContext() {
        _values.add(new Object[] { "1", "hello" });
        _values.add(new Object[] { "2", "there" });
        _values.add(new Object[] { "3", "world" });

        _table = new MutableTable("table");
        _table.addColumn(new MutableColumn("foo", ColumnType.VARCHAR).setTable(_table).setColumnNumber(0));
        _table.addColumn(new MutableColumn("bar", ColumnType.VARCHAR).setTable(_table).setColumnNumber(1));
        _schema = new MutableSchema("schema", _table);
        _table.setSchema(_schema);
    }

    public MutableTable getTable() {
        return _table;
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {

        List<Row> rows = new ArrayList<Row>();
        SelectItem[] items = MetaModelHelper.createSelectItems(columns);
        CachingDataSetHeader header = new CachingDataSetHeader(items);

        for (final Object[] values : _values) {
            Object[] rowValues = new Object[columns.length];
            for (int i = 0; i < columns.length; i++) {
                int columnNumber = columns[i].getColumnNumber();
                rowValues[i] = values[columnNumber];
            }
            rows.add(new DefaultRow(header, rowValues));
        }

        if (rows.isEmpty()) {
            return new EmptyDataSet(items);
        }
        return new InMemoryDataSet(header, rows);
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return _schema.getName();
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        return _schema;
    }

    @Override
    public void executeUpdate(UpdateScript update) {
        update.run(new AbstractUpdateCallback(this) {

            @Override
            public boolean isDeleteSupported() {
                return true;
            }

            @Override
            public RowDeletionBuilder deleteFrom(Table table) throws IllegalArgumentException, IllegalStateException,
                    UnsupportedOperationException {
                return new AbstractRowDeletionBuilder(table) {
                    @Override
                    public void execute() throws MetaModelException {
                        delete(getWhereItems());
                    }
                };
            }

            @Override
            public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException,
                    UnsupportedOperationException {
                return new AbstractRowInsertionBuilder<UpdateCallback>(this, table) {

                    @Override
                    public void execute() throws MetaModelException {
                        Object[] values = toRow().getValues();
                        _values.add(values);
                    }
                };
            }

            @Override
            public boolean isDropTableSupported() {
                return false;
            }

            @Override
            public boolean isCreateTableSupported() {
                return false;
            }

            @Override
            public TableDropBuilder dropTable(Table table) throws IllegalArgumentException, IllegalStateException,
                    UnsupportedOperationException {
                throw new UnsupportedOperationException();
            }

            @Override
            public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
                    IllegalStateException {
                throw new UnsupportedOperationException();
            }
        });
    }

    private void delete(List<FilterItem> whereItems) {
        final SelectItem[] selectItems = MetaModelHelper.createSelectItems(_table.getColumns());
        final CachingDataSetHeader header = new CachingDataSetHeader(selectItems);
        for (Iterator<Object[]> it = _values.iterator(); it.hasNext();) {
            Object[] values = (Object[]) it.next();
            DefaultRow row = new DefaultRow(header, values);
            boolean delete = true;
            for (FilterItem filterItem : whereItems) {
                if (!filterItem.evaluate(row)) {
                    delete = false;
                    break;
                }
            }
            if (delete) {
                it.remove();
            }
        }
    }

    public List<Object[]> getValues() {
        return _values;
    }
}
