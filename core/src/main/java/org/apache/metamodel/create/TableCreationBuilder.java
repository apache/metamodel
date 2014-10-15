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

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.schema.Table;

/**
 * Builder object for {@link Table} creation.
 */
public interface TableCreationBuilder {

    /**
     * Builds this table's columns based on another {@link Table} which will be
     * used as a prototype. Using this method simplifies the creation of a table
     * that is similar to an existing table.
     * 
     * @param table
     *            the {@link Table} to use as a prototype
     * @return a builder object for further table creation
     */
    public TableCreationBuilder like(Table table);

    /**
     * Adds a column to the current builder
     * 
     * @param name
     * @return
     */
    public ColumnCreationBuilder withColumn(String name);

    /**
     * Returns this builder instance as a table. This allows for inspecting the
     * table that is being built, before it is actually created.
     * 
     * @return a temporary representation of the table being built.
     */
    public Table toTable();

    /**
     * Gets a SQL representation of this create table operation. Note that the
     * generated SQL is dialect agnostic, so it is not accurately the same as
     * what will be passed to a potential backing database.
     * 
     * @return a SQL representation of this create table operation.
     */
    public String toSql();

    /**
     * Commits the built table and requests that the table structure should be
     * written to the {@link DataContext}.
     * 
     * @return the {@link Table} that was build
     * @throws MetaModelException
     *             if the {@link DataContext} was not able to create the table
     */
    public Table execute() throws MetaModelException;
}
