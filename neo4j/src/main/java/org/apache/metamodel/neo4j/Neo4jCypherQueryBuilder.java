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

import static org.apache.metamodel.neo4j.Neo4jDataContext.NEO4J_COLUMN_NAME_ID;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

public class Neo4jCypherQueryBuilder {

    public static String buildSelectQuery(final Table table, final List<Column> columns, final int firstRow,
            final int maxRows) {
        List<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());
        return buildSelectQuery(table.getName(), columnNames, firstRow, maxRows);
    }

    public static String buildSelectQuery(final String tableName, final List<String> columnNames, final int firstRow,
            final int maxRows) {
        final Map<String, String> returnClauseMap = new LinkedHashMap<>();
        final Map<String, Integer> relationshipIndexMap = new LinkedHashMap<>();
        for (String columnName : columnNames) {
            if (columnName.startsWith(Neo4jDataContext.NEO4J_COLUMN_NAME_RELATION_PREFIX)) {
                columnName = columnName.replace(Neo4jDataContext.NEO4J_COLUMN_NAME_RELATION_PREFIX, "");

                final String relationshipName;
                final String relationshipPropertyName;

                if (columnName.contains(Neo4jDataContext.NEO4J_COLUMN_NAME_RELATION_LIST_INDICATOR)) {
                    String[] parsedColumnNameArray =
                            columnName.split(Neo4jDataContext.NEO4J_COLUMN_NAME_RELATION_LIST_INDICATOR);
                    relationshipName = parsedColumnNameArray[0];
                    relationshipPropertyName = parsedColumnNameArray[1];
                } else {
                    relationshipName = columnName;
                    relationshipPropertyName = "metamodel_neo4j_relationship_marker";
                }

                final String relationshipAlias;
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
                if (columnName.equals(NEO4J_COLUMN_NAME_ID)) {
                    returnClauseMap.put(columnName, "id(n)");
                } else {
                    returnClauseMap.put(columnName, "n." + columnName);
                }
            }
        }

        final StringBuilder cypherBuilder = new StringBuilder();
        cypherBuilder.append("MATCH (n:");
        cypherBuilder.append(tableName);
        for (final Map.Entry<String, Integer> relationshipAliasEntry : relationshipIndexMap.entrySet()) {
            cypherBuilder.append(") OPTIONAL MATCH (n)-[r" + relationshipAliasEntry.getValue() + ":"
                    + relationshipAliasEntry.getKey() + "]->(r" + relationshipAliasEntry.getValue()
                    + "_relationshipEndNode");
        }
        cypherBuilder.append(") RETURN ");
        boolean addComma = false;
        for (final Map.Entry<String, String> returnClauseEntry : returnClauseMap.entrySet()) {
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

    public static String buildCountQuery(final String tableName, final List<FilterItem> whereItems) {
        StringBuilder cypherBuilder = new StringBuilder();
        cypherBuilder.append("MATCH (n:");
        cypherBuilder.append(tableName);
        cypherBuilder.append(") ");
        cypherBuilder.append(buildWhereClause(whereItems, "n"));
        cypherBuilder.append(" RETURN COUNT(*);");
        return cypherBuilder.toString();
    }

    private static String buildWhereClause(final List<FilterItem> whereItems, final String queryObjectHandle) {
        if ((whereItems != null) && (!whereItems.isEmpty())) {
            final StringBuilder whereClauseBuilder = new StringBuilder();
            whereClauseBuilder.append("WHERE ");

            final FilterItem firstWhereItem = whereItems.get(0);
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

    private static String buildWhereClauseItem(final FilterItem whereItem, final String queryObjectHandle) {
        final StringBuilder whereClauseItemBuilder = new StringBuilder();
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
