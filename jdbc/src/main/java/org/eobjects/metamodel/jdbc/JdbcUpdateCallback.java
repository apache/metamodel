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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.eobjects.metamodel.AbstractUpdateCallback;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.create.TableCreationBuilder;
import org.eobjects.metamodel.delete.RowDeletionBuilder;
import org.eobjects.metamodel.drop.TableDropBuilder;
import org.eobjects.metamodel.insert.RowInsertionBuilder;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.update.RowUpdationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class JdbcUpdateCallback extends AbstractUpdateCallback implements UpdateCallback {

    private static final Logger logger = LoggerFactory.getLogger(JdbcUpdateCallback.class);

    private final JdbcDataContext _dataContext;
    private Connection _connection;
    private String _preparedStatementSql;
    private PreparedStatement _preparedStatement;

    public JdbcUpdateCallback(JdbcDataContext dataContext) {
        super(dataContext);
        _dataContext = dataContext;
    }
    
    protected abstract void closePreparedStatement(PreparedStatement preparedStatement);

    protected abstract void executePreparedStatement(PreparedStatement preparedStatement) throws SQLException;

    public void executePreparedStatement(PreparedStatement preparedStatement, boolean reusedStatement)
            throws SQLException {
        executePreparedStatement(preparedStatement);
        if (!reusedStatement) {
            closePreparedStatement(preparedStatement);
        }
    }

    protected final Connection getConnection() {
        if (_connection == null) {
            _connection = getDataContext().getConnection();
            try {
                _connection.setAutoCommit(false);
            } catch (SQLException e) {
                throw JdbcUtils.wrapException(e, "disable auto-commit");
            }
        }
        return _connection;
    }

    public final void close(boolean success) {
        if (_connection != null) {
            if (success && _preparedStatement != null) {
                closePreparedStatement(_preparedStatement);
            }

            try {
                commitOrRollback(success);

                if (_dataContext.isDefaultAutoCommit()) {
                    try {
                        getConnection().setAutoCommit(true);
                    } catch (SQLException e) {
                        throw JdbcUtils.wrapException(e, "enable auto-commit");
                    }
                }
            } finally {
                getDataContext().close(_connection, null, null);
            }
        }
    }

    private void commitOrRollback(boolean success) {
        if (success) {
            try {
                getConnection().commit();
            } catch (SQLException e) {
                throw JdbcUtils.wrapException(e, "commit transaction");
            }
        } else {
            try {
                getConnection().rollback();
            } catch (SQLException e) {
                throw JdbcUtils.wrapException(e, "rollback transaction");
            }
        }
    }

    @Override
    public final TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
            IllegalStateException {
        return new JdbcCreateTableBuilder(this, schema, name);
    }

    @Override
    public final RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException {
        return new JdbcInsertBuilder(this, table, _dataContext.getQueryRewriter());
    }

    @Override
    public final JdbcDataContext getDataContext() {
        return _dataContext;
    }

    protected String quoteIfNescesary(String identifier) {
        if (identifier == null) {
            return null;
        }
        final String quote = _dataContext.getIdentifierQuoteString();
        if (quote == null) {
            return identifier;
        }
        boolean quotes = false;
        if (identifier.indexOf(' ') != -1 || identifier.indexOf('-') != -1) {
            quotes = true;
        } else {
            if (SqlKeywords.isKeyword(identifier)) {
                quotes = true;
            }
        }

        if (quotes) {
            identifier = quote + identifier + quote;
        }
        return identifier;
    }

    public final PreparedStatement getPreparedStatement(String sql, boolean reuseStatement) {
        final PreparedStatement preparedStatement;
        if (reuseStatement) {
            if (sql.equals(_preparedStatementSql)) {
                preparedStatement = _preparedStatement;
            } else {
                if (_preparedStatement != null) {
                    try {
                        closePreparedStatement(_preparedStatement);
                    } catch (RuntimeException e) {
                        logger.error("Exception occurred while closing prepared statement: " + _preparedStatementSql);
                        throw e;
                    }
                }
                preparedStatement = createPreparedStatement(sql);
                _preparedStatement = preparedStatement;
                _preparedStatementSql = sql;
            }
        } else {
            preparedStatement = createPreparedStatement(sql);
        }
        return preparedStatement;
    }

    private final PreparedStatement createPreparedStatement(String sql) {
        try {
            return getConnection().prepareStatement(sql);
        } catch (SQLException e) {
            throw JdbcUtils.wrapException(e, "create prepared statement for: " + sql);
        }
    }

    @Override
    public boolean isDeleteSupported() {
        return true;
    }

    @Override
    public RowDeletionBuilder deleteFrom(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new JdbcDeleteBuilder(this, table, _dataContext.getQueryRewriter());
    }

    @Override
    public boolean isDropTableSupported() {
        return true;
    }

    @Override
    public TableDropBuilder dropTable(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new JdbcDropTableBuilder(this, table, _dataContext.getQueryRewriter());
    }

    @Override
    public boolean isUpdateSupported() {
        return true;
    }

    @Override
    public RowUpdationBuilder update(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new JdbcUpdateBuilder(this, table, _dataContext.getQueryRewriter());
    }
}
