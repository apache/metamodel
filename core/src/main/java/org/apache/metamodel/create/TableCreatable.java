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
package org.apache.metamodel.create;

import org.apache.metamodel.schema.Schema;

/**
 * Interface for objects that support creating new tables.
 */
public interface TableCreatable {

    /**
     * Determines whether table creation is supported
     * 
     * @return true if table creation is supported
     */
    public boolean isCreateTableSupported();

    /**
     * Initiates the building of a table creation operation.
     * 
     * @param schema
     *            the schema to create the table in
     * @param name
     *            the name of the new table
     * @return a builder object on which the details of the table can be
     *         specified and committed.
     * @throws IllegalArgumentException
     *             if the table argument is null or invalid.
     * @throws IllegalStateException
     *             if the connection to the DataContext is read-only or another
     *             access restriction is preventing the operation.
     */
    public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
            IllegalStateException;

    /**
     * Initiates the building of a table creation operation.
     * 
     * @param schemaName
     *            the name of the schema to create the table in
     * @param tableName
     *            the name of the new table
     * @return a builder object on which the details of the table can be
     *         specified and committed.
     * @throws IllegalArgumentException
     *             if the table argument is null or invalid.
     * @throws IllegalStateException
     *             if the connection to the DataContext is read-only or another
     *             access restriction is preventing the operation.
     */
    public TableCreationBuilder createTable(String schemaName, String tableName) throws IllegalArgumentException,
            IllegalStateException;

}
