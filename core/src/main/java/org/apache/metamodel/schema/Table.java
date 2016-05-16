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

/**
 * Represents a table and it's metadata description. Tables reside within a
 * schema and contains columns and relationships to other tables.
 * 
 * @see Schema
 * @see Column
 */
public interface Table extends Comparable<Table>, Serializable, NamedStructure {

    /**
     * Gets the name of this Table
     * 
     * @return the name of this Table
     */
    @Override
    public String getName();

    /**
     * Gets the number of columns in this table.
     * 
     * @return the number of columns in this table.
     */
    public int getColumnCount();

    /**
     * Gets the columns of this table.
     * 
     * @return the columns of this table.
     */
    public Column[] getColumns();

    /**
     * Convenience method for retrieving a column by it's name.
     * 
     * @param columnName
     *            the name of the column to retrieve
     * @return the column with the given name. Returns null if no such column is
     *         found.
     */
    public Column getColumnByName(String columnName);

    /**
     * Gets a column by index. Use {@link #getColumnCount()} to get the
     * (0-based) index range.
     * 
     * @param index
     *            the index of the column
     * @return the column with the specified index
     * @throws IndexOutOfBoundsException
     *             if the index is out of bounds (index &gt;= column count)
     */
    public Column getColumn(int index) throws IndexOutOfBoundsException;

    /**
     * Gets the schema that this table resides in.
     * 
     * @return the schema that the table resides in.
     */
    public Schema getSchema();

    /**
     * Gets the table type of this table.
     * 
     * @return the type of table
     */
    public TableType getType();

    /**
     * Gets all relationships for this table.
     * 
     * @return all relationsips for this table. To add relations use
     *         TableRelation.createRelation();
     */
    public Relationship[] getRelationships();

    /**
     * Gets relationships between this table and another table.
     * 
     * @param otherTable
     *            another table for which to find relationships to and from.
     * @return an array of relationsips between this and the other table.
     */
    public Relationship[] getRelationships(Table otherTable);

    /**
     * Gets a count of relationships to and from this table.
     * 
     * @return a count of relationships to and from this table.
     */
    public int getRelationshipCount();

    /**
     * Gets remarks/comments to this table.
     * 
     * @return remarks/comments to this table or null if none exist.
     */
    public String getRemarks();

    /**
     * Gets all of this table's columns that are of number type.
     * 
     * @return an array of columns.
     * @see ColumnType
     */
    public Column[] getNumberColumns();

    /**
     * Gets all of this table's columns that are of literal (String/text) type.
     * 
     * @return an array of columns.
     * @see ColumnType
     */
    public Column[] getLiteralColumns();

    /**
     * Gets all of this table's columns that are time and/or date based.
     * 
     * @return an array of columns.
     * @see ColumnType
     */
    public Column[] getTimeBasedColumns();

    /**
     * Gets all of this table's columns that are of boolean type.
     * 
     * @return an array of columns.
     * @see ColumnType
     */
    public Column[] getBooleanColumns();

    /**
     * Gets all of this table's columns that are indexed.
     * 
     * @return an array of columns.
     */
    public Column[] getIndexedColumns();

    /**
     * @return the relationships where this table is the foreign table
     */
    public Relationship[] getForeignKeyRelationships();

    /**
     * @return the relationships where this table is the primary table
     */
    public Relationship[] getPrimaryKeyRelationships();

    /**
     * Gets the columns of this table that are known to be foreign keys (ie.
     * references primary keys in other tables).
     * 
     * @return an array of columns that are known to be foreign keys.
     */
    public Column[] getForeignKeys();

    /**
     * Gets the columns of this table that are known to be primary keys. See
     * {@link Column#isPrimaryKey()}.
     * 
     * @return an array of columns that are known to be primary keys.
     */
    public Column[] getPrimaryKeys();

    /**
     * Gets the names of this table's columns.
     * 
     * @return an array of column names.
     */
    public String[] getColumnNames();

    /**
     * Gets the columns of this table that conforms to a specified
     * {@link ColumnType}.
     * 
     * @param columnType
     *            the column type to search for.
     * @return an array of columns that match the specified ColumnType.
     */
    public Column[] getColumnsOfType(ColumnType columnType);

    /**
     * Gets the columns of this table that conforms to a specified
     * {@link SuperColumnType}.
     * 
     * @param superColumnType
     *            the super type of the column
     * @return an array of columns that match the specified SuperColumnType.
     */
    public Column[] getColumnsOfSuperType(SuperColumnType superColumnType);

}