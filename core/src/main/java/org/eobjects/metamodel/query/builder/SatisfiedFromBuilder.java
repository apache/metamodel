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
package org.eobjects.metamodel.query.builder;

import org.eobjects.metamodel.query.FunctionType;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;

/**
 * Represents a builder where the FROM part is satisfied, ie. a SELECT clause is
 * now buildable.
 * 
 * @author Kasper Sørensen
 */
public interface SatisfiedFromBuilder {

    public TableFromBuilder and(Table table);

    public TableFromBuilder and(String schemaName, String tableName);

    public TableFromBuilder and(String tableName);

    public ColumnSelectBuilder<?> select(Column column);

    public FunctionSelectBuilder<?> select(FunctionType functionType, Column column);

    public CountSelectBuilder<?> selectCount();

    public SatisfiedSelectBuilder<?> select(Column... columns);
    
    public SatisfiedSelectBuilder<?> selectAll();

    public ColumnSelectBuilder<?> select(String columnName);

    public SatisfiedSelectBuilder<?> select(String... columnNames);
}