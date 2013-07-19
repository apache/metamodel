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

import org.apache.metamodel.query.CompiledQuery;
import org.apache.metamodel.query.QueryParameter;

/**
 * Builder interface for WHERE items.
 * 
 * In addition to the {@link FilterBuilder}, the WHERE builder allows using
 * {@link QueryParameter}s as operands in the generated filters.
 * 
 * @param <B>
 */
public interface WhereBuilder<B extends SatisfiedQueryBuilder<?>> extends FilterBuilder<SatisfiedWhereBuilder<B>> {

    /**
     * Equals to a query parameter. Can be used with {@link CompiledQuery}
     * objects.
     */
    public SatisfiedWhereBuilder<B> eq(QueryParameter queryParameter);

    /**
     * Equals to a query parameter. Can be used with {@link CompiledQuery}
     * objects.
     */
    public SatisfiedWhereBuilder<B> isEquals(QueryParameter queryParameter);

    /**
     * Not equals to a query parameter. Can be used with {@link CompiledQuery}
     * objects.
     */
    public SatisfiedWhereBuilder<B> differentFrom(QueryParameter queryParameter);

    /**
     * Not equals to a query parameter. Can be used with {@link CompiledQuery}
     * objects.
     */
    public SatisfiedWhereBuilder<B> ne(QueryParameter queryParameter);

    /**
     * Greater than a query parameter. Can be used with {@link CompiledQuery}
     * objects.
     */
    public SatisfiedWhereBuilder<B> greaterThan(QueryParameter queryParameter);

    /**
     * Greater than a query parameter. Can be used with {@link CompiledQuery}
     * objects.
     */
    public SatisfiedWhereBuilder<B> gt(QueryParameter queryParameter);

    /**
     * Less than a query parameter. Can be used with {@link CompiledQuery}
     * objects.
     */
    public SatisfiedWhereBuilder<B> lessThan(QueryParameter queryParameter);

    /**
     * Less than a query parameter. Can be used with {@link CompiledQuery}
     * objects.
     */
    public SatisfiedWhereBuilder<B> lt(QueryParameter queryParameter);

    /**
     * Like a query parameter. Can be used with {@link CompiledQuery} objects.
     */
    public SatisfiedWhereBuilder<B> like(QueryParameter queryParameter);
}