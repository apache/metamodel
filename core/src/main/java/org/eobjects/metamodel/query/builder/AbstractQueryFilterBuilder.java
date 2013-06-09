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
package org.eobjects.metamodel.query.builder;

import java.util.Collection;
import java.util.Date;

import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;

abstract class AbstractQueryFilterBuilder<B> extends GroupedQueryBuilderCallback implements FilterBuilder<B> {

    protected final AbstractFilterBuilder<B> _filterBuilder;

    public AbstractQueryFilterBuilder(SelectItem selectItem, GroupedQueryBuilder queryBuilder) {
        super(queryBuilder);
        _filterBuilder = new AbstractFilterBuilder<B>(selectItem) {
            @Override
            protected B applyFilter(FilterItem filter) {
                return AbstractQueryFilterBuilder.this.applyFilter(filter);
            }
        };
    }

    protected abstract B applyFilter(FilterItem filter);

    @Override
    public B in(Collection<?> values) {
        return _filterBuilder.in(values);
    }

    @Override
    public B in(Number... numbers) {
        return _filterBuilder.in(numbers);
    }

    @Override
    public B in(String... strings) {
        return _filterBuilder.in(strings);
    }

    @Override
    public B isNull() {
        return _filterBuilder.isNull();
    }

    @Override
    public B isNotNull() {
        return _filterBuilder.isNotNull();
    }

    @Override
    public B isEquals(Column column) {
        return _filterBuilder.isEquals(column);
    }

    @Override
    public B isEquals(Date date) {
        return _filterBuilder.isEquals(date);
    }

    @Override
    public B isEquals(Number number) {
        return _filterBuilder.isEquals(number);
    }

    @Override
    public B isEquals(String string) {
        return _filterBuilder.isEquals(string);
    }

    @Override
    public B isEquals(Boolean bool) {
        return _filterBuilder.isEquals(bool);
    }
    
    @Override
    public B isEquals(Object obj) {
        return _filterBuilder.isEquals(obj);
    }

    @Override
    public B differentFrom(Column column) {
        return _filterBuilder.differentFrom(column);
    }

    @Override
    public B differentFrom(Date date) {
        return _filterBuilder.differentFrom(date);
    }

    @Override
    public B differentFrom(Number number) {
        return _filterBuilder.differentFrom(number);
    }

    @Override
    public B differentFrom(String string) {
        return _filterBuilder.differentFrom(string);
    }

    @Override
    public B differentFrom(Boolean bool) {
        return _filterBuilder.differentFrom(bool);
    }

    @Override
    public B differentFrom(Object obj) {
        return _filterBuilder.differentFrom(obj);
    }

    @Deprecated
    @Override
    public B higherThan(Column arg) {
        return _filterBuilder.higherThan(arg);
    }

    public B greaterThan(Column column) {
        return _filterBuilder.greaterThan(column);
    }
    
    @Override
    public B greaterThan(Object obj) {
        return _filterBuilder.greaterThan(obj);
    }

    @Deprecated
    @Override
    public B higherThan(Date arg) {
        return _filterBuilder.higherThan(arg);
    }

    @Override
    public B greaterThan(Date date) {
        return _filterBuilder.greaterThan(date);
    }

    @Deprecated
    @Override
    public B higherThan(Number arg) {
        return _filterBuilder.higherThan(arg);
    }

    @Override
    public B greaterThan(Number number) {
        return _filterBuilder.greaterThan(number);
    }

    @Deprecated
    @Override
    public B higherThan(String arg) {
        return _filterBuilder.higherThan(arg);
    }

    @Override
    public B greaterThan(String string) {
        return _filterBuilder.greaterThan(string);
    }

    @Override
    public B lessThan(Column column) {
        return _filterBuilder.lessThan(column);
    }

    @Override
    public B lessThan(Date date) {
        return _filterBuilder.lessThan(date);
    }

    @Override
    public B lessThan(Number number) {
        return _filterBuilder.lessThan(number);
    }

    @Override
    public B lessThan(String string) {
        return _filterBuilder.lessThan(string);
    }
    
    @Override
    public B lessThan(Object obj) {
        return _filterBuilder.lessThan(obj);
    }

    @Override
    public B like(String string) {
        return _filterBuilder.like(string);
    }

    @Override
    public B gt(Column column) {
        return greaterThan(column);
    }

    @Override
    public B gt(Date date) {
        return greaterThan(date);
    }

    @Override
    public B gt(Number number) {
        return greaterThan(number);
    }

    @Override
    public B gt(String string) {
        return greaterThan(string);
    }

    @Override
    public B lt(Column column) {
        return lessThan(column);
    }

    public B lt(Date date) {
        return lessThan(date);
    };

    public B lt(Number number) {
        return lessThan(number);
    };

    public B lt(String string) {
        return lessThan(string);
    };

    @Override
    public B eq(Boolean bool) {
        return isEquals(bool);
    }

    @Override
    public B eq(Column column) {
        return isEquals(column);
    }

    @Override
    public B eq(Date date) {
        return isEquals(date);
    }

    @Override
    public B eq(Number number) {
        return isEquals(number);
    }

    @Override
    public B eq(String string) {
        return isEquals(string);
    }

    @Override
    public B eq(Object obj) {
        return isEquals(obj);
    }

    @Override
    public B ne(Boolean bool) {
        return differentFrom(bool);
    }

    @Override
    public B ne(Column column) {
        return differentFrom(column);
    }

    @Override
    public B ne(Date date) {
        return differentFrom(date);
    }

    @Override
    public B ne(Number number) {
        return differentFrom(number);
    }

    @Override
    public B ne(String string) {
        return differentFrom(string);
    }

    @Override
    public B ne(Object obj) {
        return differentFrom(obj);
    }

    @Override
    @Deprecated
    public B equals(Boolean bool) {
        return isEquals(bool);
    }

    @Override
    @Deprecated
    public B equals(Column column) {
        return isEquals(column);
    }

    @Override
    @Deprecated
    public B equals(Date date) {
        return isEquals(date);
    }

    @Override
    @Deprecated
    public B equals(Number number) {
        return isEquals(number);
    }

    @Override
    @Deprecated
    public B equals(String string) {
        return isEquals(string);
    }
    
    @Override
    public B lt(Object obj) {
        return lessThan(obj);
    }
    
    @Override
    public B gt(Object obj) {
        return greaterThan(obj);
    }
    
}