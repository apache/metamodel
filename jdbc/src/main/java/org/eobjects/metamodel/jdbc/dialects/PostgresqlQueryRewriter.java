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
package org.eobjects.metamodel.jdbc.dialects;

import org.eobjects.metamodel.jdbc.JdbcDataContext;
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

/**
 * Query rewriter for PostgreSQL
 */
public class PostgresqlQueryRewriter extends LimitOffsetQueryRewriter implements IQueryRewriter {

    public PostgresqlQueryRewriter(JdbcDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public ColumnType getColumnType(int jdbcType, String nativeType, Integer columnSize) {
        if ("bool".equals(nativeType)) {
            // override the normal behaviour of postgresql which maps "bool" to
            // a BIT.
            return ColumnType.BOOLEAN;
        }
        return super.getColumnType(jdbcType, nativeType, columnSize);
    }

    @Override
    public String rewriteColumnType(ColumnType columnType) {
        if (columnType == ColumnType.BLOB) {
            return "bytea";
        }
        return super.rewriteColumnType(columnType);
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