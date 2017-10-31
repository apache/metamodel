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

import java.sql.SQLException;

import org.apache.metamodel.MetaModelException;

/**
 * Represents an exception during {@link JdbcDataContext#executeUpdate(org.apache.metamodel.UpdateScript)} which was
 * rolled back at the JDBC layer. This particular exception type can typically be catched and the update can be retried
 * to retry the transaction (assuming that the script is designed in a way that makes it idempotent and has not
 * side-effects on state outside of the {@link JdbcDataContext}).
 * 
 * Note that the cause of this will always be a {@link SQLException} since it only applies to JDBC.
 */
public class RolledBackUpdateException extends MetaModelException {

    private static final long serialVersionUID = 1L;

    public RolledBackUpdateException(SQLException cause) {
        super(cause);
    }

    public RolledBackUpdateException(String message, SQLException cause) {
        super(message, cause);
    }

    @Override
    public synchronized SQLException getCause() {
        return (SQLException) super.getCause();
    }
}
