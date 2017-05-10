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

import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.schema.ColumnType;

/**
 * Query rewriter for MySQL
 */
public class MysqlQueryRewriter extends LimitOffsetQueryRewriter {

    public MysqlQueryRewriter(JdbcDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public String escapeQuotes(String filterItemOperand) {
        return filterItemOperand.replaceAll("\\'", "\\\\'");
    }

    @Override
    public String rewriteColumnType(ColumnType columnType, Integer columnSize) {
        if (columnType == ColumnType.NUMERIC) {
            return super.rewriteColumnType(ColumnType.DECIMAL, columnSize);
        }
        if (columnType.isLiteral() && columnSize == null) {
            if (columnType == ColumnType.STRING || columnType == ColumnType.VARCHAR
                    || columnType == ColumnType.NVARCHAR) {
                // MySQL requires size to be specified, so instead we choose the
                // text type
                return "TEXT";
            }
        }
        return super.rewriteColumnType(columnType, columnSize);
    }
}