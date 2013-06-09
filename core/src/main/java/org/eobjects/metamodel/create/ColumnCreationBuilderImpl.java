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

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.Table;

/**
 * Implementation of the {@link ColumnCreationBuilder}.
 * 
 * @author Kasper Sørensen
 */
final class ColumnCreationBuilderImpl extends AbstractColumnBuilder<ColumnCreationBuilder> implements ColumnCreationBuilder {

    private final TableCreationBuilder _createTableBuilder;

    public ColumnCreationBuilderImpl(TableCreationBuilder createTableBuilder, MutableColumn column) {
        super(column);
        _createTableBuilder = createTableBuilder;
    }

    @Override
    public String toSql() {
        return _createTableBuilder.toSql();
    }

    @Override
    public TableCreationBuilder like(Table table) {
        return _createTableBuilder.like(table);
    }

    @Override
    public Table execute() throws MetaModelException {
        return _createTableBuilder.execute();
    }

    @Override
    public ColumnCreationBuilder withColumn(String name) {
        return _createTableBuilder.withColumn(name);
    }

    @Override
    public Table toTable() {
        return _createTableBuilder.toTable();
    }
}
