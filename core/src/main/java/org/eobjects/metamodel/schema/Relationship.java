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

/**
 * Represents a relationship between two tables where one set of columns is the
 * primary key, and another set is the foreign key.
 * 
 * @see Table
 * @see Column
 * 
 * @author Kasper Sørensen
 */
public interface Relationship extends Serializable, Comparable<Relationship> {

	/**
	 * Gets the table of the primary key column(s).
	 * 
	 * @return the table of the primary key column(s).
	 */
	public Table getPrimaryTable();

	/**
	 * Gets the primary key columns of this relationship.
	 * 
	 * @return an array of primary key columns.
	 */
	public Column[] getPrimaryColumns();

	/**
	 * Gets the table of the foreign key column(s).
	 * 
	 * @return the table of the foreign key column(s).
	 */
	public Table getForeignTable();

	/**
	 * Gets the foreign key columns of this relationship.
	 * 
	 * @return an array of foreign key columns.
	 */
	public Column[] getForeignColumns();

	/**
	 * Determines whether this relationship contains a specific pair of columns
	 * 
	 * @param pkColumn
	 *            primary key column
	 * @param fkColumn
	 *            foreign key column
	 * @return true if this relation contains the specified primary and foreign
	 *         columns as a part of the relation
	 */
	public boolean containsColumnPair(Column pkColumn, Column fkColumn);

}