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
package org.eobjects.metamodel.create;

import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.schema.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract {@link TableCreationBuilder} implementation, provided as convenience
 * for {@link TableCreatable} implementations. Handles all the building
 * operations, but not the commit operation.
 * 
 * @author Kasper Sørensen
 */
public abstract class AbstractTableCreationBuilder<U extends UpdateCallback> implements TableCreationBuilder {

    private static final Logger logger = LoggerFactory.getLogger(AbstractTableCreationBuilder.class);

    private final U _updateCallback;
    private final MutableTable _table;
    private final Schema _schema;

    public AbstractTableCreationBuilder(U updateCallback, Schema schema, String name) {
        if (schema != null && schema.getTableByName(name) != null) {
            throw new IllegalArgumentException("A table with the name '" + name + "' already exists in schema: "
                    + schema);
        }
        _updateCallback = updateCallback;
        _schema = schema;
        _table = new MutableTable(name, TableType.TABLE, schema);
    }

    protected U getUpdateCallback() {
        return _updateCallback;
    }

    protected Schema getSchema() {
        return _schema;
    }

    protected MutableTable getTable() {
        return _table;
    }

    @Override
    public Table toTable() {
        return _table;
    }

    @Override
    public TableCreationBuilder like(Table table) {
        logger.debug("like({})", table);
        Column[] columns = table.getColumns();
        for (Column column : columns) {
            withColumn(column.getName()).like(column);
        }
        return this;
    }

    @Override
    public ColumnCreationBuilder withColumn(String name) {
        logger.debug("withColumn({})", name);
        MutableColumn col = (MutableColumn) _table.getColumnByName(name);
        if (col == null) {
            col = new MutableColumn(name).setTable(_table).setColumnNumber(_table.getColumnCount());
            _table.addColumn(col);
        }
        return new ColumnCreationBuilderImpl(this, col);
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ");
        sb.append(_table.getQualifiedLabel());
        sb.append(" (");
        Column[] columns = _table.getColumns();
        for (int i = 0; i < columns.length; i++) {
            if (i != 0) {
                sb.append(',');
            }
            Column column = columns[i];
            sb.append(column.getName());
            ColumnType type = column.getType();
            if (type != null) {
                sb.append(' ');
                sb.append(type.toString());
                Integer columnSize = column.getColumnSize();
                if (columnSize != null) {
                    sb.append('(');
                    sb.append(columnSize);
                    sb.append(')');
                }
            }
            if (column.isNullable() != null
                    && !column.isNullable().booleanValue()) {
                sb.append(" NOT NULL");
            }
            if (column.isPrimaryKey()) {
                sb.append(" PRIMARY KEY");
            }
        }
        sb.append(")");
        return sb.toString();
    }
}
