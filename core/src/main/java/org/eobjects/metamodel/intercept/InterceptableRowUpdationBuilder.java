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
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.data.Style;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.query.builder.AbstractFilterBuilder;
import org.eobjects.metamodel.query.builder.FilterBuilder;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.update.RowUpdationBuilder;

final class InterceptableRowUpdationBuilder implements RowUpdationBuilder {

    private final RowUpdationBuilder _rowUpdationBuilder;
    private final InterceptorList<RowUpdationBuilder> _rowUpdationInterceptors;

    public InterceptableRowUpdationBuilder(RowUpdationBuilder rowUpdationBuilder,
            InterceptorList<RowUpdationBuilder> rowUpdationInterceptors) {
        _rowUpdationBuilder = rowUpdationBuilder;
        _rowUpdationInterceptors = rowUpdationInterceptors;
    }

    @Override
    public RowUpdationBuilder value(int columnIndex, Object value) {
        _rowUpdationBuilder.value(columnIndex, value);
        return this;
    }

    @Override
    public RowUpdationBuilder value(int columnIndex, Object value, Style style) {
        _rowUpdationBuilder.value(columnIndex, value, style);
        return this;
    }

    @Override
    public RowUpdationBuilder value(Column column, Object value) {
        _rowUpdationBuilder.value(column, value);
        return this;
    }

    @Override
    public RowUpdationBuilder value(Column column, Object value, Style style) {
        _rowUpdationBuilder.value(column, value, style);
        return this;
    }

    @Override
    public RowUpdationBuilder value(String columnName, Object value) {
        _rowUpdationBuilder.value(columnName, value);
        return this;
    }

    @Override
    public RowUpdationBuilder value(String columnName, Object value, Style style) {
        _rowUpdationBuilder.value(columnName, value, style);
        return this;
    }

    @Override
    public Row toRow() {
        return _rowUpdationBuilder.toRow();
    }

    @Override
    public boolean isSet(Column column) {
        return _rowUpdationBuilder.isSet(column);
    }

    @Override
    public FilterBuilder<RowUpdationBuilder> where(Column column) {
        final SelectItem selectItem = new SelectItem(column);
        return new AbstractFilterBuilder<RowUpdationBuilder>(selectItem) {
            @Override
            protected RowUpdationBuilder applyFilter(FilterItem filter) {
                where(filter);
                return InterceptableRowUpdationBuilder.this;
            }
        };
    }

    @Override
    public FilterBuilder<RowUpdationBuilder> where(String columnName) {
        Column column = getTable().getColumnByName(columnName);
        return where(column);
    }

    @Override
    public RowUpdationBuilder where(FilterItem... filterItems) {
        _rowUpdationBuilder.where(filterItems);
        return this;
    }

    @Override
    public RowUpdationBuilder where(Iterable<FilterItem> filterItems) {
        _rowUpdationBuilder.where(filterItems);
        return this;
    }

    @Override
    public String toSql() {
        return _rowUpdationBuilder.toSql();
    }

    @Override
    public Table getTable() {
        return _rowUpdationBuilder.getTable();
    }

    @Override
    public void execute() throws MetaModelException {
        RowUpdationBuilder rowUpdationBuilder = _rowUpdationBuilder;
        rowUpdationBuilder = _rowUpdationInterceptors.interceptAll(rowUpdationBuilder);
        rowUpdationBuilder.execute();
    }

}
