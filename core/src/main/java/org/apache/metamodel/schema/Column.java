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
 * Represents a column and it's metadata description. Columns reside within a
 * Table and can be used as keys for relationships between tables.
 * 
 * @see Table
 */
public interface Column extends Comparable<Column>, Serializable, NamedStructure {

    /**
     * Gets the name of this Column
     * 
     * @return the name of this Column
     */
    @Override
    public String getName();

    /**
     * Returns the column number or index. Note: This column number is 0-based
     * whereas the JDBC is 1-based.
     * 
     * @return the number of this column.
     */
    public int getColumnNumber();

    /**
     * Gets the type of the column
     * 
     * @return this column's type.
     */
    public ColumnType getType();

    /**
     * Gets the table for which this column belong
     * 
     * @return this column's table.
     */
    public Table getTable();

    /**
     * Determines whether or not this column accepts null values.
     * 
     * @return true if this column accepts null values, false if not and null if
     *         not known.
     */
    public Boolean isNullable();

    /**
     * Gets any remarks/comments to this column.
     * 
     * @return any remarks/comments to this column.
     */
    public String getRemarks();

    /**
     * Gets the data type size of this column.
     * 
     * @return the data type size of this column or null if the size is not
     *         determined or known.
     */
    public Integer getColumnSize();

    /**
     * Gets the native type of this column. A native type is the name of the
     * data type as defined in the datastore.
     * 
     * @return the name of the native type.
     */
    public String getNativeType();

    /**
     * Determines if this column is indexed.
     * 
     * @return true if this column is indexed or false if not (or not known)
     */
    public boolean isIndexed();

    /**
     * Determines if this column is (one of) the primary key(s) of its table.
     * 
     * @return true if this column is a primary key, or false if not (or if this
     *         is not determinable).
     */
    public boolean isPrimaryKey();
}