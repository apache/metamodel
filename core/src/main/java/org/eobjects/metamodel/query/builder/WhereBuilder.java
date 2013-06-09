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

import org.eobjects.metamodel.query.CompiledQuery;
import org.eobjects.metamodel.query.QueryParameter;

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