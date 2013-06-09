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
package org.eobjects.metamodel.schema;

import java.io.Serializable;

import org.eobjects.metamodel.DataContext;

/**
 * Represents a schema and it's metadata description. Schemas represent a
 * collection of tables.
 * 
 * @see DataContext
 * @see Table
 * 
 * @author Kasper Sørensen
 */
public interface Schema extends Comparable<Schema>, Serializable, NamedStructure {

	/**
	 * Gets the name of this Schema
	 * 
	 * @return the name of this Schema
	 */
	@Override
	public String getName();

	/**
	 * Gets the number of tables that reside in this schema.
	 * 
	 * @return the number of tables that reside in this schema.
	 */
	public int getTableCount();

	/**
	 * Gets the number of tables in this Schema that comply to a given
	 * TableType.
	 * 
	 * @param type
	 *            the TableType to use for searching and matching.
	 * @return the count of tables that match the provided TableType.
	 */
	public int getTableCount(TableType type);

	/**
	 * Gets the names of the tables that reside in this Schema.
	 * 
	 * @return an array of table names.
	 */
	public String[] getTableNames();

	/**
	 * Gets all tables in this Schema.
	 * 
	 * @return the tables that reside in the schema
	 */
	public Table[] getTables();

	/**
	 * Gets all tables in this Schema of a particular type.
	 * 
	 * @param type
	 *            the TableType to use for searching and matching.
	 * @return an array of tables in this schema that matches the specified
	 *         type.
	 */
	public Table[] getTables(TableType type);

	/**
	 * Gets a table by index. Use {@link #getTableCount()} to get the (0-based)
	 * index range
	 * 
	 * @param index
	 *            the index of the table
	 * @return the column with the specified index
	 * 
	 * @throws IndexOutOfBoundsException
	 *             if the index is out of bounds (index >= table count)
	 */
	public Table getTable(int index) throws IndexOutOfBoundsException;

	/**
	 * Convenience method for retrieving a table by it's name.
	 * 
	 * @param tableName
	 *            the name of the table to retrieve
	 * @return the table with the given name. Returns null if no such table is
	 *         found.
	 */
	public Table getTableByName(String tableName);

	/**
	 * Gets all relationships to and from this Schema.
	 * 
	 * @return an array of relationships.
	 */
	public Relationship[] getRelationships();

	/**
	 * Gets the number of relationships to and from this Schema.
	 * 
	 * @return the number of relationships to and from this Schema
	 */
	public int getRelationshipCount();
}