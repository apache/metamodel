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
package org.eobjects.metamodel;

import java.util.ArrayList;
import java.util.List;

import org.eobjects.metamodel.data.CachingDataSetHeader;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.DataSetHeader;
import org.eobjects.metamodel.data.DefaultRow;
import org.eobjects.metamodel.data.EmptyDataSet;
import org.eobjects.metamodel.data.InMemoryDataSet;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

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
        primaryTable.addColumn(new MutableColumn("foo").setColumnNumber(0).setType(ColumnType.VARCHAR).setTable(primaryTable));
        primaryTable.addColumn(new MutableColumn("bar").setColumnNumber(1).setType(ColumnType.VARCHAR).setTable(primaryTable));
        primaryTable.addColumn(new MutableColumn("baz").setColumnNumber(2).setType(ColumnType.VARCHAR).setTable(primaryTable));

        final MutableTable emptyTable = new MutableTable("an_empty_table").setSchema(schema);
        emptyTable.addColumn(new MutableColumn("foo").setColumnNumber(0).setType(ColumnType.VARCHAR).setTable(emptyTable));
        emptyTable.addColumn(new MutableColumn("bar").setColumnNumber(1).setType(ColumnType.VARCHAR).setTable(emptyTable));
        
        schema.addTable(primaryTable);
        schema.addTable(emptyTable);
        
        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return _schemaName;
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        if (_tableName.equals(table.getName())) {
            final SelectItem[] allSelectItems = MetaModelHelper.createSelectItems(table.getColumns());
            final DataSetHeader header = new CachingDataSetHeader(allSelectItems);
            final List<Row> data = new ArrayList<Row>();
            data.add(new DefaultRow(header, new Object[] { "1", "hello", "world" }, null));
            data.add(new DefaultRow(header, new Object[] { "2", _value, "world" }, null));
            data.add(new DefaultRow(header, new Object[] { "3", "hi", _value }, null));
            data.add(new DefaultRow(header, new Object[] { "4", "yo", "world" }, null));

            DataSet ds = new InMemoryDataSet(header, data);

            SelectItem[] columnSelectItems = MetaModelHelper.createSelectItems(columns);
            ds = MetaModelHelper.getSelection(columnSelectItems, ds);

            return ds;
        } else if ("an_empty_table".equals(table.getName())) {
            return new EmptyDataSet(columns);
        }
        throw new UnsupportedOperationException();
    }

}
