/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jdbc {@link UpdateCallback} for databases that support the JDBC Batch
 * features.
 * 
 * @author Kasper Sørensen
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
