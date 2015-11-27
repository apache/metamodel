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
package org.apache.metamodel.elasticsearch.common;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.LogicalOperator;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.util.CollectionUtils;
import org.elasticsearch.common.base.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchUtils {
    public static final String FIELD_ID = "_id";
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchUtils.class);

    /**
     * Gets a "filter" query which is both 1.x and 2.x compatible.
     */
    private static QueryBuilder getFilteredQuery(String prefix, String fieldName) {
        // 1.x: itemQueryBuilder = QueryBuilders.filteredQuery(null, FilterBuilders.missingFilter(fieldName));
        // 2.x: itemQueryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.missingQuery(fieldName));
        try {
            try {
                Method method = QueryBuilders.class.getDeclaredMethod(prefix + "Query", String.class);
                method.setAccessible(true);
                return QueryBuilders.boolQuery().must((QueryBuilder) method.invoke(null, fieldName));
            } catch (NoSuchMethodException e) {
                Class<?> clazz = ElasticSearchUtils.class.getClassLoader()
                        .loadClass("org.elasticsearch.index.query.FilterBuilders");
                Method filterBuilderMethod = clazz.getDeclaredMethod(prefix + "Filter", String.class);
                filterBuilderMethod.setAccessible(true);
                Method queryBuildersFilteredQueryMethod =
                        QueryBuilders.class.getDeclaredMethod("filteredQuery", QueryBuilder.class, FilterBuilder.class);
                return (QueryBuilder) queryBuildersFilteredQueryMethod.invoke(null, null,
                        filterBuilderMethod.invoke(null, fieldName));
            }
        } catch (Exception e) {
            logger.error("Failed to resolve/invoke filtering method", e);
            throw new IllegalStateException("Failed to resolve filtering method", e);
        }
    }

    public static QueryBuilder getMissingQuery(String fieldName) {
        return getFilteredQuery("missing", fieldName);
    }

    public static QueryBuilder getExistsQuery(String fieldName) {
        return getFilteredQuery("exists", fieldName);
    }

    public static List<Object> getSourceProperties(final MutableTable table) {
        if (table.getColumnByName(FIELD_ID) == null) {
            final MutableColumn idColumn = new MutableColumn(FIELD_ID, ColumnType.STRING)
                    .setTable(table).setPrimaryKey(true);
            table.addColumn(0, idColumn);
        }

        final List<Object> sourceProperties = new ArrayList<>();

        for (Column column : table.getColumns()) {
            // each column is defined as a property pair of the form: ("field1",
            // "type=string,store=true")
            final String columnName = column.getName();
            if (FIELD_ID.equals(columnName)) {
                // do nothing - the ID is a client-side construct
                continue;
            }
            sourceProperties.add(columnName);

            String type = getType(column);
            if (type == null) {
                sourceProperties.add("store=true");
            } else {
                sourceProperties.add("type=" + type + ",store=true");
            }
        }

        return sourceProperties;
    }

    /**
     * Determines the best fitting type. For reference of ElasticSearch types,
     * see
     *
     * <pre>
     * http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/mapping-core-types.html
     * </pre>
     *
     *
     * @param column
     * @return
     */
    private static String getType(Column column) {
        String nativeType = column.getNativeType();
        if (!Strings.isNullOrEmpty(nativeType)) {
            return nativeType;
        }

        final ColumnType type = column.getType();
        if (type == null) {
            throw new IllegalStateException("No column type specified for '" + column.getName()
                    + "' - cannot build ElasticSearch mapping without type.");
        }

        if (type.isLiteral()) {
            return "string";
        } else if (type == ColumnType.FLOAT) {
            return "float";
        } else if (type == ColumnType.DOUBLE || type == ColumnType.NUMERIC || type == ColumnType.NUMBER) {
            return "double";
        } else if (type == ColumnType.SMALLINT) {
            return "short";
        } else if (type == ColumnType.TINYINT) {
            return "byte";
        } else if (type == ColumnType.INTEGER) {
            return "integer";
        } else if (type == ColumnType.DATE || type == ColumnType.TIMESTAMP) {
            return "date";
        } else if (type == ColumnType.BINARY || type == ColumnType.VARBINARY) {
            return "binary";
        } else if (type == ColumnType.BOOLEAN || type == ColumnType.BIT) {
            return "boolean";
        } else if (type == ColumnType.MAP) {
            return "object";
        }

        throw new UnsupportedOperationException("Unsupported column type '" + type.getName() + "' of column '"
                + column.getName() + "' - cannot translate to an ElasticSearch type.");
    }

    /**
     * Creates, if possible, a {@link QueryBuilder} object which can be used to
     * push down one or more {@link FilterItem}s to ElasticSearch's backend.
     *
     * @return a {@link QueryBuilder} if one was produced, or null if the items
     *         could not be pushed down to an ElasticSearch query
     */
    public static QueryBuilder createQueryBuilderForSimpleWhere(List<FilterItem> whereItems,
            LogicalOperator logicalOperator) {
        if (whereItems.isEmpty()) {
            return QueryBuilders.matchAllQuery();
        }

        List<QueryBuilder> children = new ArrayList<>(whereItems.size());
        for (FilterItem item : whereItems) {
            final QueryBuilder itemQueryBuilder;

            if (item.isCompoundFilter()) {
                final List<FilterItem> childItems = Arrays.asList(item.getChildItems());
                itemQueryBuilder = createQueryBuilderForSimpleWhere(childItems, item.getLogicalOperator());
                if (itemQueryBuilder == null) {
                    // something was not supported, so we have to forfeit here
                    // too.
                    return null;
                }
            } else {
                final Column column = item.getSelectItem().getColumn();
                if (column == null) {
                    // unsupported type of where item - must have a column
                    // reference
                    return null;
                }
                final String fieldName = column.getName();
                final Object operand = item.getOperand();
                final OperatorType operator = item.getOperator();

                if (OperatorType.EQUALS_TO.equals(operator)) {
                    if (operand == null) {
                        itemQueryBuilder = getMissingQuery(fieldName);
                    } else {
                        itemQueryBuilder = QueryBuilders.termQuery(fieldName, operand);
                    }
                } else if (OperatorType.DIFFERENT_FROM.equals(operator)) {
                    if (operand == null) {
                        itemQueryBuilder = getExistsQuery(fieldName);
                    } else {
                        itemQueryBuilder = QueryBuilders.boolQuery().mustNot(
                                QueryBuilders.termQuery(fieldName, operand));
                    }
                } else if (OperatorType.IN.equals(operator)) {
                    final List<?> operands = CollectionUtils.toList(operand);
                    itemQueryBuilder = QueryBuilders.termsQuery(fieldName, operands);
                } else {
                    // not (yet) support operator types
                    return null;
                }
            }

            children.add(itemQueryBuilder);
        }

        // just one where item - just return the child query builder
        if (children.size() == 1) {
            return children.get(0);
        }

        // build a bool query
        final BoolQueryBuilder result = QueryBuilders.boolQuery();
        for (QueryBuilder child : children) {
            switch (logicalOperator) {
            case AND:
                result.must(child);
            case OR:
                result.should(child);
            }
        }

        return result;
    }

    public static ColumnType getColumnTypeFromElasticSearchType(final String metaDataFieldType) {
        final ColumnType columnType;
        if (metaDataFieldType.startsWith("date")) {
            columnType = ColumnType.DATE;
        } else if (metaDataFieldType.equals("long")) {
            columnType = ColumnType.BIGINT;
        } else if (metaDataFieldType.equals("string")) {
            columnType = ColumnType.STRING;
        } else if (metaDataFieldType.equals("float")) {
            columnType = ColumnType.FLOAT;
        } else if (metaDataFieldType.equals("boolean")) {
            columnType = ColumnType.BOOLEAN;
        } else if (metaDataFieldType.equals("double")) {
            columnType = ColumnType.DOUBLE;
        } else {
            columnType = ColumnType.STRING;
        }
        return columnType;
    }
}
