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
package org.apache.metamodel.jdbc.dialects;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.postgresql.util.PGobject;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Query rewriter for PostgreSQL
 */
public class PostgresqlQueryRewriter extends LimitOffsetQueryRewriter {

    private final ObjectMapper jsonObjectMapper = new ObjectMapper();

    public PostgresqlQueryRewriter(JdbcDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public ColumnType getColumnType(int jdbcType, String nativeType, Integer columnSize) {
        switch (nativeType) {
        case "bool":
            // override the normal behaviour of postgresql which maps "bool" to
            // a BIT.
            return ColumnType.BOOLEAN;
        case "json":
        case "jsonb":
            return ColumnType.MAP;
        }
        return super.getColumnType(jdbcType, nativeType, columnSize);
    }

    @Override
    public String rewriteColumnType(ColumnType columnType, Integer columnSize) {
        if (columnType == ColumnType.BLOB) {
            return "bytea";
        }
        if (columnType == ColumnType.BIT) {
            return "BOOLEAN";
        }
        if (columnType == ColumnType.DOUBLE) {
            return "double precision";
        }
        if (columnType == ColumnType.MAP) {
            return "jsonb";
        }
        return super.rewriteColumnType(columnType, columnSize);
    }

    @Override
    public void setStatementParameter(PreparedStatement st, int valueIndex, Column column, Object value)
            throws SQLException {
        switch (column.getNativeType()) {
        case "json":
        case "jsonb":
            assert column.getType() == ColumnType.MAP;
            if (value == null) {
                st.setObject(valueIndex, null);
            } else {
                final PGobject pgo = new PGobject();
                pgo.setType(column.getNativeType());
                if (value instanceof Map) {
                    try {
                        pgo.setValue(jsonObjectMapper.writeValueAsString(value));
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Unable to write value as JSON string: " + value);
                    }
                } else {
                    pgo.setValue(value.toString());
                }
                st.setObject(valueIndex, pgo);
            }
            return;
        }
        super.setStatementParameter(st, valueIndex, column, value);
    }

    @Override
    public Object getResultSetValue(ResultSet resultSet, int columnIndex, Column column) throws SQLException {
        switch (column.getNativeType()) {
        case "json":
        case "jsonb":
            assert column.getType() == ColumnType.MAP;
            final String stringValue = resultSet.getString(columnIndex);
            if (stringValue == null) {
                return null;
            }
            try {
                return jsonObjectMapper.readValue(stringValue, Map.class);
            } catch (Exception e) {
                throw new IllegalArgumentException("Unable to read string as JSON: " + stringValue);
            }
        }
        return super.getResultSetValue(resultSet, columnIndex, column);
    }

    @Override
    protected String rewriteFromItem(Query query, FromItem item) {
        String result = super.rewriteFromItem(query, item);
        Table table = item.getTable();
        if (table != null) {
            Schema schema = table.getSchema();
            if (schema != null) {
                String schemaName = schema.getName();
                if (schemaName != null) {
                    result = result.replaceFirst(schemaName, '\"' + schema.getName() + '\"');
                }
            }
        }
        return result;
    }
}