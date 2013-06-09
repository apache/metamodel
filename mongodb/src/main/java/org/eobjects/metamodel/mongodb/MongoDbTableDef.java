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
package org.eobjects.metamodel.mongodb;

import java.io.Serializable;

import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.util.SimpleTableDef;

/**
 * Defines a table layout for {@link MongoDbDataContext} tables. This class can
 * be used as an instruction set for the {@link MongoDbDataContext} to specify
 * which collections, which columns (and their types) should be included in the
 * schema structure of a Mongo DB database.
 * 
 * @author Kasper Sørensen
 * 
 * @deprecated use {@link SimpleTableDef} instead.
 */
@Deprecated
public final class MongoDbTableDef extends SimpleTableDef implements Serializable {

	private static final long serialVersionUID = 1L;

	public MongoDbTableDef(String name, String[] columnNames, ColumnType[] columnTypes) {
		super(name, columnNames, columnTypes);
	}

	public MongoDbTableDef(String name, String[] columnNames) {
		super(name, columnNames);
	}
}
