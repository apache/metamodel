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
package org.apache.metamodel.schema;

import java.io.Serializable;

import org.apache.metamodel.DataContext;

/**
 * Represents a schema and it's metadata description. Schemas represent a
 * collection of tables.
 * 
 * @see DataContext
 * @see Table
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
	 *             if the index is out of bounds (index &gt;= table count)
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