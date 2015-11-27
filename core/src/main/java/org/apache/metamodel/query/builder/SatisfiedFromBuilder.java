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
package org.apache.metamodel.query.builder;

import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

/**
 * Represents a builder where the FROM part is satisfied, ie. a SELECT clause is
 * now buildable.
 */
public interface SatisfiedFromBuilder {

    public TableFromBuilder and(Table table);

    public TableFromBuilder and(String schemaName, String tableName);

    public TableFromBuilder and(String tableName);

    public ColumnSelectBuilder<?> select(Column column);
    
    public FunctionSelectBuilder<?> select(FunctionType function, String columnName);

    public FunctionSelectBuilder<?> select(FunctionType function, Column column);
    
    public FunctionSelectBuilder<?> select(FunctionType function, String columnName, Object[] functionParameters);

    public FunctionSelectBuilder<?> select(FunctionType function, Column column, Object[] functionParameters);

    public CountSelectBuilder<?> selectCount();

    public SatisfiedSelectBuilder<?> select(Column... columns);
    
    public SatisfiedSelectBuilder<?> selectAll();

    public SatisfiedSelectBuilder<?> select(String selectExpression);
    
    public SatisfiedSelectBuilder<?> select(String selectExpression, boolean allowExpressionBasedSelectItem);

    public SatisfiedSelectBuilder<?> select(String... columnNames);
}