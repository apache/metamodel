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
import org.eobjects.metamodel.schema.Table;

/**
 * Represents a builder where the FROM part is satisfied, ie. a SELECT clause is
 * now buildable.
 * 
 * @author Kasper Sørensen
 */
public interface SatisfiedFromBuilder {

    public TableFromBuilder and(Table table);

    public TableFromBuilder and(String schemaName, String tableName);

    public TableFromBuilder and(String tableName);

    public ColumnSelectBuilder<?> select(Column column);

    public FunctionSelectBuilder<?> select(FunctionType functionType, Column column);

    public CountSelectBuilder<?> selectCount();

    public SatisfiedSelectBuilder<?> select(Column... columns);
    
    public SatisfiedSelectBuilder<?> selectAll();

    public ColumnSelectBuilder<?> select(String columnName);

    public SatisfiedSelectBuilder<?> select(String... columnNames);
}