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

import org.eobjects.metamodel.query.FunctionType;
import org.eobjects.metamodel.schema.Column;

class SatisfiedSelectBuilderImpl extends GroupedQueryBuilderCallback implements
		SatisfiedSelectBuilder<GroupedQueryBuilder> {

	public SatisfiedSelectBuilderImpl(GroupedQueryBuilder queryBuilder) {
		super(queryBuilder);
	}

	@Override
	public ColumnSelectBuilder<GroupedQueryBuilder> and(Column column) {
		if (column == null) {
			throw new IllegalArgumentException("column cannot be null");
		}
		return getQueryBuilder().select(column);
	}

	@Override
	public SatisfiedSelectBuilder<GroupedQueryBuilder> and(Column... columns) {
		if (columns == null) {
			throw new IllegalArgumentException("columns cannot be null");
		}
		return getQueryBuilder().select(columns);
	}

	@Override
	public FunctionSelectBuilder<GroupedQueryBuilder> and(
			FunctionType functionType, Column column) {
		if (functionType == null) {
			throw new IllegalArgumentException("functionType cannot be null");
		}
		if (column == null) {
			throw new IllegalArgumentException("column cannot be null");
		}
		return getQueryBuilder().select(functionType, column);
	}

	@Override
	public SatisfiedSelectBuilder<GroupedQueryBuilder> and(String columnName) {
		if (columnName == null) {
			throw new IllegalArgumentException("columnName cannot be null");
		}
		return getQueryBuilder().select(columnName);
	}

}