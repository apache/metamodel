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
import java.sql.Statement;

import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jdbc {@link UpdateCallback} for databases that support the JDBC Batch
 * features.
 */
final class JdbcBatchUpdateCallback extends JdbcUpdateCallback {

    private static final Logger logger = LoggerFactory.getLogger(JdbcBatchUpdateCallback.class);

    public JdbcBatchUpdateCallback(JdbcDataContext dataContext) {
        super(dataContext);
    }

    @Override
    protected void closePreparedStatement(PreparedStatement preparedStatement) {
        try {
            int[] results = preparedStatement.executeBatch();
            if (logger.isDebugEnabled()) {
                for (int i = 0; i < results.length; i++) {
                    int result = results[i];
                    final String resultString;
                    switch (result) {
                    case Statement.SUCCESS_NO_INFO:
                        resultString = "SUCCESS_NO_INFO";
                        break;
                    case Statement.EXECUTE_FAILED:
                        resultString = "EXECUTE_FAILED";
                        break;
                    default:
                        resultString = result + " rows updated";
                    }
                    logger.debug("batch execute result[" + i + "]:" + resultString);
                }
            }
        } catch (SQLException e) {
            throw JdbcUtils.wrapException(e, "execute batch: " + preparedStatement);
        } finally {
            FileHelper.safeClose(preparedStatement);
        }
    }

    @Override
    protected void executePreparedStatement(PreparedStatement st) throws SQLException {
        st.addBatch();
    }
}
