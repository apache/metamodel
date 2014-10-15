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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.util.FileHelper;

/**
 * Jdbc {@link UpdateCallback} for databases that do not support batch features.
 * Instead we will use a single transaction for the {@link UpdateScript}.
 */
final class JdbcSimpleUpdateCallback extends JdbcUpdateCallback {

    public JdbcSimpleUpdateCallback(JdbcDataContext dataContext) {
        super(dataContext);
    }
    
    @Override
    protected void closePreparedStatement(PreparedStatement preparedStatement) {
        FileHelper.safeClose(preparedStatement);
    }

    @Override
    protected void executePreparedStatement(PreparedStatement st) throws SQLException {
        st.executeUpdate();
    }
}
