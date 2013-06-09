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
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.Table;

final class InterceptableColumnCreationBuilder implements ColumnCreationBuilder {

	private final ColumnCreationBuilder _columnCreationBuilder;
	private final InterceptableTableCreationBuilder _tableCreationBuilder;

	public InterceptableColumnCreationBuilder(
			ColumnCreationBuilder columnCreationBuilder,
			InterceptableTableCreationBuilder tableCreationBuilder) {
		_columnCreationBuilder = columnCreationBuilder;
		_tableCreationBuilder = tableCreationBuilder;
	}
	
	@Override
	public String toSql() {
	    return _tableCreationBuilder.toSql();
	}

	@Override
	public TableCreationBuilder like(Table table) {
		return _tableCreationBuilder.like(table);
	}

	@Override
	public ColumnCreationBuilder withColumn(String name) {
		_columnCreationBuilder.withColumn(name);
		return this;
	}

	@Override
	public Table toTable() {
		return _tableCreationBuilder.toTable();
	}

	@Override
	public Table execute() throws MetaModelException {
		return _tableCreationBuilder.execute();
	}

	@Override
	public ColumnCreationBuilder like(Column column) {
		_columnCreationBuilder.like(column);
		return this;
	}
	
	@Override
	public ColumnCreationBuilder asPrimaryKey() {
        _columnCreationBuilder.asPrimaryKey();
        return this;
	}

	@Override
	public ColumnCreationBuilder ofType(ColumnType type) {
		_columnCreationBuilder.ofType(type);
		return this;
	}

	@Override
	public ColumnCreationBuilder ofNativeType(String nativeType) {
		_columnCreationBuilder.ofNativeType(nativeType);
		return this;
	}

	@Override
	public ColumnCreationBuilder ofSize(int size) {
		_columnCreationBuilder.ofSize(size);
		return this;
	}

	@Override
	public ColumnCreationBuilder nullable(boolean nullable) {
		_columnCreationBuilder.nullable(nullable);
		return this;
	}

}
