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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

public class Neo4jCypherQueryBuilder {

    public static String buildSelectQuery(Table table, List<Column> columns, int firstRow, int maxRows) {
        List<String> columnNames = columns.stream()
                .map(col -> col.getName())
                .collect(Collectors.toList());
        return buildSelectQuery(table.getName(), columnNames, firstRow, maxRows);
    }

    public static String buildSelectQuery(String tableName, List<String> columnNames, int firstRow, int maxRows) {
        Map<String, String> returnClauseMap = new LinkedHashMap<>();
        Map<String, Integer> relationshipIndexMap = new LinkedHashMap<>();
        for (String columnName : columnNames) {
            if (columnName.startsWith(Neo4jDataContext.RELATIONSHIP_PREFIX)) {
                columnName = columnName.replace(Neo4jDataContext.RELATIONSHIP_PREFIX, "");

                String relationshipName;
                String relationshipPropertyName;

                if (columnName.contains(Neo4jDataContext.RELATIONSHIP_COLUMN_SEPARATOR)) {
                    String[] parsedColumnNameArray = columnName.split(Neo4jDataContext.RELATIONSHIP_COLUMN_SEPARATOR);

                    relationshipName = parsedColumnNameArray[0];
                    relationshipPropertyName = parsedColumnNameArray[1];
                } else {
                    relationshipName = columnName;
                    relationshipPropertyName = "metamodel_neo4j_relationship_marker";
                }

                String relationshipAlias;
                if (relationshipIndexMap.containsKey(relationshipName)) {
                    relationshipAlias = "r" + relationshipIndexMap.get(relationshipName);
                } else {
                    int nextIndex;
                    if (relationshipIndexMap.values().isEmpty()) {
                        nextIndex = 0;
                    } else {
                        nextIndex = Collections.max(relationshipIndexMap.values()) + 1;
                    }
                    relationshipIndexMap.put(relationshipName, nextIndex);
                    relationshipAlias = "r" + relationshipIndexMap.get(relationshipName);
                }

                if (relationshipPropertyName.equals("metamodel_neo4j_relationship_marker")) {
                    returnClauseMap.put(columnName, "id(" + relationshipAlias + "_relationshipEndNode)");
                } else {
                    returnClauseMap.put(columnName, relationshipAlias + "." + relationshipPropertyName);
                }
            } else {
                if (columnName.equals("_id")) {
                    returnClauseMap.put(columnName, "id(n)");
                } else {
                    returnClauseMap.put(columnName, "n." + columnName);
                }
            }
        }

        StringBuilder cypherBuilder = new StringBuilder();
        cypherBuilder.append("MATCH (n:");
        cypherBuilder.append(tableName);
        for (Map.Entry<String, Integer> relationshipAliasEntry : relationshipIndexMap.entrySet()) {
            cypherBuilder.append(") OPTIONAL MATCH (n)-[r" + relationshipAliasEntry.getValue() + ":"
                    + relationshipAliasEntry.getKey() + "]->(r" + relationshipAliasEntry.getValue()
                    + "_relationshipEndNode");
        }
        cypherBuilder.append(") RETURN ");
        boolean addComma = false;
        for (Map.Entry<String, String> returnClauseEntry : returnClauseMap.entrySet()) {
            if (addComma) {
                cypherBuilder.append(",");
            }
            cypherBuilder.append(returnClauseEntry.getValue());
            addComma = true;
        }

        if (firstRow > 1) {
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
        cypherBuilder.append(") ");
        cypherBuilder.append(buildWhereClause(whereItems, "n"));
        cypherBuilder.append(" RETURN COUNT(*);");
        return cypherBuilder.toString();
    }

    private static String buildWhereClause(List<FilterItem> whereItems, String queryObjectHandle) {
        if ((whereItems != null) && (!whereItems.isEmpty())) {
            StringBuilder whereClauseBuilder = new StringBuilder();
            whereClauseBuilder.append("WHERE ");

            FilterItem firstWhereItem = whereItems.get(0);
            whereClauseBuilder.append(buildWhereClauseItem(firstWhereItem, queryObjectHandle));

            for (int i = 1; i < whereItems.size(); i++) {
                whereClauseBuilder.append(" AND ");
                FilterItem whereItem = whereItems.get(i);
                whereClauseBuilder.append(buildWhereClauseItem(whereItem, queryObjectHandle));
            }

            return whereClauseBuilder.toString();
        } else {
            return "";
        }
    }

    private static String buildWhereClauseItem(FilterItem whereItem, String queryObjectHandle) {
        StringBuilder whereClauseItemBuilder = new StringBuilder();
        whereClauseItemBuilder.append(queryObjectHandle);
        whereClauseItemBuilder.append(".");
        whereClauseItemBuilder.append(whereItem.getSelectItem().getColumn().getName());
        whereClauseItemBuilder.append(whereItem.getOperator().toSql());
        final Object operand = whereItem.getOperand();
        if (operand instanceof String) {
            whereClauseItemBuilder.append("\"");
        }
        whereClauseItemBuilder.append(operand);
        if (operand instanceof String) {
            whereClauseItemBuilder.append("\"");
        }
        return whereClauseItemBuilder.toString();
    }

}
