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
package org.eobjects.metamodel.intercept;

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.delete.RowDeletionBuilder;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.query.builder.AbstractFilterBuilder;
import org.eobjects.metamodel.query.builder.FilterBuilder;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;

final class InterceptableRowDeletionBuilder implements RowDeletionBuilder {

    private final RowDeletionBuilder _rowDeletionBuilder;
    private final InterceptorList<RowDeletionBuilder> _rowDeletionInterceptors;

    public InterceptableRowDeletionBuilder(RowDeletionBuilder rowDeletionBuilder,
            InterceptorList<RowDeletionBuilder> rowDeletionInterceptors) {
        _rowDeletionBuilder = rowDeletionBuilder;
        _rowDeletionInterceptors = rowDeletionInterceptors;
    }

    @Override
    public FilterBuilder<RowDeletionBuilder> where(Column column) {
        final SelectItem selectItem = new SelectItem(column);
        return new AbstractFilterBuilder<RowDeletionBuilder>(selectItem) {
            @Override
            protected RowDeletionBuilder applyFilter(FilterItem filter) {
                return where(filter);
            }
        };
    }

    @Override
    public FilterBuilder<RowDeletionBuilder> where(String columnName) {
        Column column = getTable().getColumnByName(columnName);
        return where(column);
    }

    @Override
    public RowDeletionBuilder where(FilterItem... filterItems) {
        _rowDeletionBuilder.where(filterItems);
        return this;
    }

    @Override
    public RowDeletionBuilder where(Iterable<FilterItem> filterItems) {
        _rowDeletionBuilder.where(filterItems);
        return this;
    }

    @Override
    public Table getTable() {
        return _rowDeletionBuilder.getTable();
    }

    @Override
    public String toSql() {
        return _rowDeletionBuilder.toSql();
    }

    @Override
    public void execute() throws MetaModelException {
        RowDeletionBuilder rowDeletionBuilder = _rowDeletionBuilder;
        rowDeletionBuilder = _rowDeletionInterceptors.interceptAll(rowDeletionBuilder);
        rowDeletionBuilder.execute();
    }

}
