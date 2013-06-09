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

import java.util.ArrayList;
import java.util.List;

import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.FunctionType;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;

final class HavingBuilderImpl extends
		AbstractQueryFilterBuilder<SatisfiedHavingBuilder> implements
		HavingBuilder, SatisfiedHavingBuilder {

	private final Query _query;
	private final List<FilterItem> _orFilters;
	private FilterItem _parentOrFilter;

	public HavingBuilderImpl(FunctionType function, Column column, Query query,
			GroupedQueryBuilder queryBuilder) {
		super(new SelectItem(function, column), queryBuilder);
		_query = query;
		_orFilters = new ArrayList<FilterItem>();
	}

	public HavingBuilderImpl(FunctionType function, Column column, Query query,
			FilterItem parentOrFilter, List<FilterItem> orFilters,
			GroupedQueryBuilder queryBuilder) {
		super(new SelectItem(function, column), queryBuilder);
		_query = query;
		_orFilters = orFilters;
		_parentOrFilter = parentOrFilter;
	}

	@Override
	protected SatisfiedHavingBuilder applyFilter(FilterItem filter) {
		if (_parentOrFilter == null) {
			_query.having(filter);
		} else {
			if (_parentOrFilter.getChildItemCount() == 1) {
				_query.getHavingClause().removeItem(_orFilters.get(0));
				_query.getHavingClause().addItem(_parentOrFilter);
			}
		}
		_orFilters.add(filter);
		return this;
	}

	@Override
	public HavingBuilder or(FunctionType function, Column column) {
		if (function == null) {
			throw new IllegalArgumentException("function cannot be null");
		}
		if (column == null) {
			throw new IllegalArgumentException("column cannot be null");
		}
		if (_parentOrFilter == null) {
			_parentOrFilter = new FilterItem(_orFilters);
		}
		return new HavingBuilderImpl(function, column, _query, _parentOrFilter,
				_orFilters, getQueryBuilder());
	}

	@Override
	public HavingBuilder and(FunctionType function, Column column) {
		return getQueryBuilder().having(function, column);
	}
}