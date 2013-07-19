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
package org.apache.metamodel.update;

import org.apache.metamodel.schema.Table;

public interface RowUpdateable {

    /**
     * Determines whether row update is supported
     * 
     * @return true if row update is supported
     */
    public boolean isUpdateSupported();

    /**
     * Initiates a row updation builder.
     * 
     * @param table
     * @return
     * @throws IllegalArgumentException
     * @throws IllegalStateException
     * @throws UnsupportedOperationException
     */
    public RowUpdationBuilder update(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException;

    /**
     * Initiates a row updation builder.
     * 
     * @param tableName
     * @return
     * @throws IllegalArgumentException
     * @throws IllegalStateException
     * @throws UnsupportedOperationException
     */
    public RowUpdationBuilder update(String tableName) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException;

    /**
     * Initiates a row updation builder.
     * 
     * @param schemaName
     * @param tableName
     * @return
     * @throws IllegalArgumentException
     * @throws IllegalStateException
     * @throws UnsupportedOperationException
     */
    public RowUpdationBuilder update(String schemaName, String tableName) throws IllegalArgumentException,
            IllegalStateException, UnsupportedOperationException;
}
