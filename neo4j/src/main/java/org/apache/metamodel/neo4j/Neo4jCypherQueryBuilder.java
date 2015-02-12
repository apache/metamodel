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

import java.util.List;

import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

public class Neo4jCypherQueryBuilder {

    public static String buildSelectQuery(Table table, Column[] columns, int firstRow, int maxRows) {
        String[] columnNames = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {
            columnNames[i] = columns[i].getName();
        }
        return buildSelectQuery(table.getName(), columnNames, firstRow, maxRows);
    }
    
    public static String buildSelectQuery(String tableName, String[] columnNames, int firstRow, int maxRows) {
        StringBuilder cypherBuilder = new StringBuilder();
        cypherBuilder.append("MATCH (n:");
        cypherBuilder.append(tableName);
        cypherBuilder.append(") RETURN ");
        boolean addComma = false;
        for (String columnName : columnNames) {
            if (addComma) {
                cypherBuilder.append(",");
            }
            cypherBuilder.append("n.");
            cypherBuilder.append(columnName);
            addComma = true;
        }
        if (firstRow > 0) {
            cypherBuilder.append(" SKIP " + (firstRow - 1));
        }
        if (maxRows > -1) {
            cypherBuilder.append(" LIMIT " + maxRows);
        }
        return cypherBuilder.toString();
    }
    
    public static String buildCountQuery(String tableName, List<FilterItem> whereItems) {
        StringBuilder cypherBuilder = new StringBuilder();
        cypherBuilder.append("MATCH (n:");
        cypherBuilder.append(tableName);
        cypherBuilder.append(") RETURN COUNT(*);");
        return cypherBuilder.toString();
    }

    public static String buildCountQuery(Table table, List<FilterItem> whereItems) {
        return buildCountQuery(table.getName(), whereItems);
    }
    
}
