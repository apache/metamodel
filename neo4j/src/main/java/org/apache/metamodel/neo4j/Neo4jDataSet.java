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
package org.apache.metamodel.neo4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.Query;

final class Neo4jDataSet extends AbstractDataSet {

    private Neo4jDataContext _neo4jDataContext;
    private Connection _connection;
    private Statement _statement;
    private ResultSet _resultSet;
    private Row _row;

    public Neo4jDataSet(Query query, Neo4jDataContext neo4jDataContext, Connection connection, Statement statement,
            ResultSet resultSet) {
        super(query.getSelectClause().getItems());
        if (query == null || statement == null || resultSet == null) {
            throw new IllegalArgumentException("Arguments cannot be null");
        }
        _neo4jDataContext = neo4jDataContext;
        _connection = connection;
        _statement = statement;
        _resultSet = resultSet;
    }
    
    @Override
    public boolean next() {
        try {
            boolean result = _resultSet.next();
            if (result) {
                Object[] values = new Object[getHeader().size()];
                for (int i = 0; i < values.length; i++) {

                    values[i] = getValue(_resultSet, i);
                }
                _row = new DefaultRow(getHeader(), values);
            } else {
                _row = null;
            }
            return result;
        } catch (SQLException e) {
            //throw JdbcUtils.wrapException(e, "get next record in resultset");
        }
        return false;
    }
    
    private Object getValue(ResultSet resultSet, int i) throws SQLException {
        final int columnIndex = i + 1;
        return _resultSet.getObject(columnIndex);
    }

    @Override
    public Row getRow() {
        return _row;
    }

}
