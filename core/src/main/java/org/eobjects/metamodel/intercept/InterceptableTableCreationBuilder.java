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
import org.eobjects.metamodel.create.ColumnCreationBuilder;
import org.eobjects.metamodel.create.TableCreationBuilder;
import org.eobjects.metamodel.schema.Table;

final class InterceptableTableCreationBuilder implements TableCreationBuilder {

	private final TableCreationBuilder _tabelCreationBuilder;
	private final InterceptorList<TableCreationBuilder> _tableCreationInterceptors;

	public InterceptableTableCreationBuilder(
			TableCreationBuilder tabelCreationBuilder,
			InterceptorList<TableCreationBuilder> tableCreationInterceptors) {
		_tabelCreationBuilder = tabelCreationBuilder;
		_tableCreationInterceptors = tableCreationInterceptors;
	}
	
	@Override
	public String toSql() {
	    return _tabelCreationBuilder.toSql();
	}

	@Override
	public TableCreationBuilder like(Table table) {
		_tabelCreationBuilder.like(table);
		return this;
	}

	@Override
	public ColumnCreationBuilder withColumn(String name) {
		ColumnCreationBuilder columnCreationBuilder = _tabelCreationBuilder
				.withColumn(name);
		return new InterceptableColumnCreationBuilder(columnCreationBuilder,
				this);
	}

	@Override
	public Table toTable() {
		return _tabelCreationBuilder.toTable();
	}

	@Override
	public Table execute() throws MetaModelException {
		TableCreationBuilder tableCreationBuilder = _tabelCreationBuilder;
		tableCreationBuilder = _tableCreationInterceptors
				.interceptAll(tableCreationBuilder);
		return tableCreationBuilder.execute();
	}
}
