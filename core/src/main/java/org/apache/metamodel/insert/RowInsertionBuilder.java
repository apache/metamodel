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

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.RowBuilder;
import org.apache.metamodel.schema.Table;

/**
 * Builder object for row insertion, into a {@link Table}.
 */
public interface RowInsertionBuilder extends RowBuilder<RowInsertionBuilder> {

    /**
     * Gets the table that this insert pertains to.
     * 
     * @return the table that this insert pertains to.
     */
    @Override
    public Table getTable();

    /**
     * Sets all values like the provided row (for easy duplication of a row).
     * 
     * @param row
     *            the row from which to take values
     * @return the builder itself
     */
    public RowInsertionBuilder like(Row row);

    /**
     * Commits the row insertion operation. This operation will write the row to
     * the {@link DataContext}.
     * 
     * @throws MetaModelException
     *             if the operation was rejected
     */
    public void execute() throws MetaModelException;

    /**
     * Gets a SQL representation of this insert operation. Note that the
     * generated SQL is dialect agnostic, so it is not accurately the same as
     * what will be passed to a potential backing database.
     * 
     * @return a SQL representation of this insert operation.
     */
    public String toSql();
}
