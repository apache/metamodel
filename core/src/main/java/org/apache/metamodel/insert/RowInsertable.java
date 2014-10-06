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
package org.apache.metamodel.insert;

import org.apache.metamodel.schema.Table;

/**
 * An interface for objects that support inserting rows into tables.
 */
public interface RowInsertable {

    /**
     * Determines whether row insertion is supported
     * 
     * @return true if row insertion is supported
     */
    public boolean isInsertSupported();

    /**
     * Initiates the building of a row insertion operation.
     * 
     * @param table
     *            the table to insert a row into
     * @return a builder object on which values can be added and the statement
     *         can be committed.
     * @throws IllegalArgumentException
     *             if the table argument is null or invalid.
     * @throws IllegalStateException
     *             if the connection to the DataContext is read-only or another
     *             access restriction is preventing the operation.
     * @throws UnsupportedOperationException
     *             in case {@link #isInsertSupported()} is false
     */
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException;

    /**
     * Initiates the building of a row insertion operation.
     * 
     * @param tableName
     *            the name of the table to insert a row into
     * @return a builder object on which values can be added and the statement
     *         can be committed.
     * @throws IllegalArgumentException
     *             if the tableName argument is null or invalid.
     * @throws IllegalStateException
     *             if the connection to the DataContext is read-only or another
     *             access restriction is preventing the operation.
     * @throws UnsupportedOperationException
     *             in case {@link #isInsertSupported()} is false
     */
    public RowInsertionBuilder insertInto(String tableName) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException;

    /**
     * Initiates the building of a row insertion operation.
     * 
     * @param schemaName
     *            the name of the schema
     * @param tableName
     *            the name of the table to insert a row into
     * @return a builder object on which values can be added and the statement
     *         can be committed.
     * @throws IllegalArgumentException
     *             if the tableName argument is null or invalid.
     * @throws IllegalStateException
     *             if the connection to the DataContext is read-only or another
     *             access restriction is preventing the operation.
     * @throws UnsupportedOperationException
     *             in case {@link #isInsertSupported()} is false
     */
    public RowInsertionBuilder insertInto(String schemaName, String tableName) throws IllegalArgumentException,
            IllegalStateException, UnsupportedOperationException;
}
