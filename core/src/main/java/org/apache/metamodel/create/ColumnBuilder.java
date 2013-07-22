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
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;

/**
 * Abstract interface for components that build columns.
 * 
 * Defines methods for refining particular properties of the column build.
 * 
 * @param <T>
 *            the return type of the builder methods
 */
public interface ColumnBuilder<T extends ColumnBuilder<?>> {

    /**
     * Builds several properties of a column, based on another {@link Column}
     * object as a prototype.
     * 
     * @param column
     *            a prototype for the column being built.
     * @return a builder object for further column creation.
     */
    public T like(Column column);

    /**
     * Defines the {@link ColumnType} of the created column.
     * 
     * @param type
     *            the column type of the created column.
     * @return a builder object for further column creation.
     */
    public T ofType(ColumnType type);

    /**
     * Defines the native type of the created column (useful especially for SQL
     * based {@link DataContext}s).
     * 
     * @param nativeType
     *            the native type of the created column
     * @return a builder object for further column creation.
     */
    public T ofNativeType(String nativeType);

    /**
     * Defines the size of the created column.
     * 
     * @param size
     *            the size of the created column.
     * @return a builder object for further column creation.
     */
    public T ofSize(int size);

    /**
     * Defines if the created column should be nullable or not.
     * 
     * @param nullable
     *            if the created column should be nullable or not.
     * @return a builder object for further column creation.
     */
    public T nullable(boolean nullable);

    /**
     * Defines that the created column should be a primary key
     * 
     * @return a builder object for further column creation.
     */
    public T asPrimaryKey();
}
