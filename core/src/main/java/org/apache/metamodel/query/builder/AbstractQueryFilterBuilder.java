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
package org.apache.metamodel.query.builder;

import java.util.Collection;
import java.util.Date;

import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;

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
    public B notIn(Collection<?> values) {
        return _filterBuilder.notIn(values);
    }

    @Override
    public B notIn(Number... numbers) {
        return _filterBuilder.notIn(numbers);
    }

    @Override
    public B notIn(String... strings) {
        return _filterBuilder.notIn(strings);
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
    public B greaterThanOrEquals(Column column) {
        return _filterBuilder.greaterThanOrEquals(column);
    }

    @Override
    public B greaterThanOrEquals(Date date) {
        return _filterBuilder.greaterThanOrEquals(date);
    }

    @Override
    public B greaterThanOrEquals(Number number) {
        return _filterBuilder.greaterThanOrEquals(number);
    }

    @Override
    public B greaterThanOrEquals(String string) {
        return _filterBuilder.greaterThanOrEquals(string);
    }

    @Override
    public B greaterThanOrEquals(Object obj) {
        return _filterBuilder.greaterThanOrEquals(obj);
    }

    @Override
    public B gte(Column column) {
        return _filterBuilder.greaterThanOrEquals(column);
    }

    @Override
    public B gte(Date date) {
        return _filterBuilder.greaterThanOrEquals(date);
    }

    @Override
    public B gte(Number number) {
        return _filterBuilder.greaterThanOrEquals(number);
    }

    @Override
    public B gte(String string) {
        return _filterBuilder.greaterThanOrEquals(string);
    }

    @Override
    public B gte(Object obj) {
        return _filterBuilder.greaterThanOrEquals(obj);
    }

    @Override
    public B lessThanOrEquals(Column column) {
        return _filterBuilder.lessThanOrEquals(column);
    }

    @Override
    public B lessThanOrEquals(Date date) {
        return _filterBuilder.lessThanOrEquals(date);
    }

    @Override
    public B lessThanOrEquals(Number number) {
        return _filterBuilder.lessThanOrEquals(number);
    }

    @Override
    public B lessThanOrEquals(String string) {
        return _filterBuilder.lessThanOrEquals(string);
    }

    @Override
    public B lessThanOrEquals(Object obj) {
        return _filterBuilder.lessThanOrEquals(obj);
    }

    @Override
    public B lte(Column column) {
        return _filterBuilder.lessThanOrEquals(column);
    }

    @Override
    public B lte(Date date) {
        return _filterBuilder.lessThanOrEquals(date);
    }

    @Override
    public B lte(Number number) {
        return _filterBuilder.lessThanOrEquals(number);
    }

    @Override
    public B lte(String string) {
        return _filterBuilder.lessThanOrEquals(string);
    }

    @Override
    public B lte(Object obj) {
        return _filterBuilder.lessThanOrEquals(obj);
    }

    @Override
    public B like(String string) {
        return _filterBuilder.like(string);
    }

    @Override
    public B notLike(String string) {
        return _filterBuilder.notLike(string);
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
    }

    public B lt(Number number) {
        return lessThan(number);
    }

    public B lt(String string) {
        return lessThan(string);
    }

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