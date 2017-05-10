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

import org.apache.metamodel.schema.Column;

/**
 * Interface for builder callbacks that "respond" to filter condition building.
 *
 * @param <B>
 *            the builder type to return once filter has been created.
 */
public interface FilterBuilder<B> {

    /**
     * Not null
     */
    public B isNull();

    /**
     * Is not null
     */
    public B isNotNull();

    /**
     * In ...
     */
    public B in(Collection<?> values);

    /**
     * In ...
     */
    public B in(Number... numbers);

    /**
     * In ...
     */
    public B in(String... strings);

    /**
     * Not in ...
     */
    public B notIn(Collection<?> values);

    /**
     * Not in ...
     */
    public B notIn(Number... numbers);

    /**
     * Not in ...
     */
    public B notIn(String... strings);

    /**
     * Like ...
     *
     * (use '%' as wildcard).
     */
    public B like(String string);

    /**
     * Not like ...
     *
     * (use '%' as wildcard).
     */
    public B notLike(String string);

    /**
     * Equal to ...
     */
    public B eq(Column column);

    /**
     * Equal to ...
     */
    public B eq(Date date);

    /**
     * Equal to ...
     */
    public B eq(Number number);

    /**
     * Equal to ...
     */
    public B eq(String string);

    /**
     * Equal to ...
     */
    public B eq(Boolean bool);

    /**
     * Equal to ...
     */
    public B eq(Object obj);

    /**
     * Equal to ...
     */
    public B isEquals(Column column);

    /**
     * Equal to ...
     */
    public B isEquals(Date date);

    /**
     * Equal to ...
     */
    public B isEquals(Number number);

    /**
     * Equal to ...
     */
    public B isEquals(String string);

    /**
     * Equal to ...
     */
    public B isEquals(Boolean bool);

    /**
     * Equal to ...
     */
    public B isEquals(Object obj);

    /**
     * Not equal to ...
     */
    public B differentFrom(Column column);

    /**
     * Not equal to ...
     */
    public B differentFrom(Date date);

    /**
     * Not equal to ...
     */
    public B differentFrom(Number number);

    /**
     * Not equal to ...
     */
    public B differentFrom(String string);

    /**
     * Not equal to ...
     */
    public B differentFrom(Boolean bool);

    /**
     * Not equal to ...
     */
    public B differentFrom(Object obj);

    /**
     * Not equal to ...
     */
    public B ne(Column column);

    /**
     * Not equal to ...
     */
    public B ne(Date date);

    /**
     * Not equal to ...
     */
    public B ne(Number number);

    /**
     * Not equal to ...
     */
    public B ne(String string);

    /**
     * Not equal to ...
     */
    public B ne(Boolean bool);

    /**
     * Not equal to ...
     */
    public B ne(Object obj);

    /**
     * Greater than ...
     */
    public B greaterThan(Column column);

    /**
     * Greater than ...
     */
    public B gt(Column column);

    /**
     * Greater than ...
     */
    public B greaterThan(Object obj);

    /**
     * Greater than ...
     */
    public B gt(Object obj);

    /**
     * Greater than ...
     */
    public B greaterThan(Date date);

    /**
     * Greater than ...
     */
    public B gt(Date date);

    /**
     * Greater than ...
     */
    public B greaterThan(Number number);

    /**
     * Greater than ...
     */
    public B gt(Number number);

    /**
     * Greater than ...
     */
    public B greaterThan(String string);

    /**
     * Greater than ...
     */
    public B gt(String string);

    /**
     * Less than ...
     */
    public B lessThan(Column column);

    /**
     * Less than ...
     */
    public B lt(Column column);

    /**
     * Less than ...
     */
    public B lessThan(Date date);

    /**
     * Less than ...
     */
    public B lessThan(Number number);

    /**
     * Less than ...
     */
    public B lessThan(String string);

    /**
     * Less than ...
     */
    public B lessThan(Object obj);

    /**
     * Less than ...
     */
    public B lt(Object obj);

    /**
     * Less than ...
     */
    public B lt(Date date);

    /**
     * Less than ...
     */
    public B lt(Number number);

    /**
     * Less than ...
     */
    public B lt(String string);

    /**
     * Greater than or equals...
     */
    public B greaterThanOrEquals(Column column);

    /**
     * Greater than or equals...
     */
    public B gte(Column column);

    /**
     * Greater than or equals...
     */
    public B greaterThanOrEquals(Date date);

    /**
     * Greater than or equals...
     */
    public B gte(Date date);

    /**
     * Greater than or equals...
     */
    public B greaterThanOrEquals(Number number);

    /**
     * Greater than or equals...
     */
    public B gte(Number number);

    /**
     * Greater than or equals...
     */
    public B greaterThanOrEquals(String string);

    /**
     * Greater than or equals...
     */
    public B gte(String string);

    /**
     * Greater than or equals...
     */
    public B greaterThanOrEquals(Object obj);

    /**
     * Greater than or equals...
     */
    public B gte(Object obj);

    /**
     * Less than or equals...
     */
    public B lessThanOrEquals(Column column);

    /**
     * Less than or equals...
     */
    public B lte(Column column);

    /**
     * Less than or equals...
     */
    public B lessThanOrEquals(Date date);

    /**
     * Less than or equals...
     */
    public B lte(Date date);

    /**
     * Less than or equals...
     */
    public B lessThanOrEquals(Number number);

    /**
     * Less than or equals...
     */
    public B lte(Number number);

    /**
     * Less than or equals...
     */
    public B lessThanOrEquals(String string);

    /**
     * Less than or equals...
     */
    public B lte(String string);

    /**
     * Less than or equals...
     */
    public B lessThanOrEquals(Object obj);

    /**
     * Less than or equals...
     */
    public B lte(Object obj);
}
