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
package org.apache.metamodel.jdbc;

import java.sql.Connection;

/**
 * Defines the interface for a component capable of loading schema-model
 * metadata.
 */
interface MetadataLoader {

    public void loadTables(JdbcSchema jdbcSchema);

    public void loadRelations(JdbcSchema jdbcSchema);

    public void loadColumns(JdbcTable jdbcTable);

    public void loadIndexes(JdbcTable jdbcTable);

    public void loadPrimaryKeys(JdbcTable jdbcTable);
    
    public void loadTables(JdbcSchema jdbcSchema, Connection connection);

    public void loadRelations(JdbcSchema jdbcSchema, Connection connection);

    public void loadColumns(JdbcTable jdbcTable, Connection connection);

    public void loadIndexes(JdbcTable jdbcTable, Connection connection);

    public void loadPrimaryKeys(JdbcTable jdbcTable, Connection connection);
}
