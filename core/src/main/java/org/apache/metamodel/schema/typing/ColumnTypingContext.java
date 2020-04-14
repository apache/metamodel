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
package org.apache.metamodel.schema.typing;

import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;

/**
 * Defines the context for configuring the type for a single column in a
 * {@link ColumnTypingStrategy} session.
 */
public interface ColumnTypingContext {

    /**
     * Gets the index of the column being configured.
     *
     * @return the column index
     */
    int getColumnIndex();

    /**
     * Gets the {@link Table} that the column is to pertain to. If the table is
     * not yet available then this may return null.
     *
     * @return the associated table
     */
    Table getTable();
}
