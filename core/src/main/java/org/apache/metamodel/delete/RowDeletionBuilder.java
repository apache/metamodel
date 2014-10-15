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
package org.apache.metamodel.delete;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.WhereClauseBuilder;
import org.apache.metamodel.schema.Table;

/**
 * Builder object for row deletions in a {@link Table}.
 */
public interface RowDeletionBuilder extends WhereClauseBuilder<RowDeletionBuilder> {

    /**
     * Gets the table that this delete statement pertains to.
     * 
     * @return the table that this delete statement pertains to.
     */
    public Table getTable();

    /**
     * Gets a SQL representation of this delete operation. Note that the
     * generated SQL is dialect agnostic, so it is not accurately the same as
     * what will be passed to a potential backing database.
     * 
     * @return a SQL representation of this delete operation.
     */
    public String toSql();

    /**
     * Commits the row deletion operation. This operation will delete rows in
     * the {@link DataContext}.
     * 
     * @throws MetaModelException
     *             if the operation was rejected
     */
    public void execute() throws MetaModelException;
}
