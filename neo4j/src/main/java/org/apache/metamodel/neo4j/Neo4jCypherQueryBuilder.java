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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
        // Split node property columns and relationship columns
        List<String> nodePropertyColumnNames = new ArrayList<String>();
        Map<String, List<String>> relationships = new LinkedHashMap<>();
        for (String columnName : columnNames) {
            if (columnName.startsWith(Neo4jDataContext.RELATIONSHIP_PREFIX)) {
                columnName = columnName.replace(Neo4jDataContext.RELATIONSHIP_PREFIX, "");

                if (columnName.contains(Neo4jDataContext.RELATIONSHIP_COLUMN_SEPARATOR)) {
                    String[] parsedColumnNameArray = columnName.split("#");

                    String relationshipName = parsedColumnNameArray[0];
                    String relationshipColumnName = parsedColumnNameArray[1];

                    if (relationships.containsKey(relationshipName)) {
                        List<String> relationshipColumnNames = relationships.get(relationshipName);
                        relationshipColumnNames.add(relationshipColumnName);
                    } else {
                        List<String> relationshipColumnNames = new ArrayList<>();
                        relationshipColumnNames.add(relationshipColumnName);
                        relationships.put(relationshipName, relationshipColumnNames);
                    }
                } else {
                    String relationshipName = columnName;
                    if (relationships.containsKey(relationshipName)) {
                        List<String> relationshipColumnNames = relationships.get(relationshipName);
                        relationshipColumnNames.add("metamodel_neo4j_relationship_marker");
                    } else {
                        List<String> relationshipColumnNames = new ArrayList<>();
                        relationshipColumnNames.add("metamodel_neo4j_relationship_marker");
                        relationships.put(relationshipName, relationshipColumnNames);
                    }
                }

            } else {
                nodePropertyColumnNames.add(columnName);
            }
        }

        StringBuilder cypherBuilder = new StringBuilder();
        cypherBuilder.append("MATCH (n:");
        cypherBuilder.append(tableName);
        int i = 0;
        for (String relationship : relationships.keySet()) {
            i++;
            cypherBuilder.append(") OPTIONAL MATCH (n)-[r" + i + ":" + relationship + "]->(r" + i
                    + "_relationshipEndNode");
        }
        cypherBuilder.append(") RETURN ");
        boolean addComma = false;
        for (String nodePropertyColumnName : nodePropertyColumnNames) {
            if (addComma) {
                cypherBuilder.append(",");
            }
            if (nodePropertyColumnName.equals("_id")) {
                cypherBuilder.append("id(n)");
            } else {
                cypherBuilder.append("n.");
                cypherBuilder.append(nodePropertyColumnName);
            }
            addComma = true;
        }
        int k = 0;
        for (Map.Entry<String, List<String>> relationshipEntrySet : relationships.entrySet()) {
            if (relationshipEntrySet.getValue().contains("metamodel_neo4j_relationship_marker")) {
                k++;
                if (addComma) {
                    cypherBuilder.append(",");
                }
                cypherBuilder.append("id(r" + k + "_relationshipEndNode)");
                addComma = true;
            }
        }
        int j = 0;
        for (Map.Entry<String, List<String>> relationshipEntrySet : relationships.entrySet()) {
            j++;
            for (String relationshipColumnName : relationshipEntrySet.getValue()) {
                if (!relationshipColumnName.equals("metamodel_neo4j_relationship_marker")) {
                    if (addComma) {
                        cypherBuilder.append(",");
                    }
                    cypherBuilder.append("r" + j + ".");
                    cypherBuilder.append(relationshipColumnName);
                    addComma = true;
                    
                }
            }

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
