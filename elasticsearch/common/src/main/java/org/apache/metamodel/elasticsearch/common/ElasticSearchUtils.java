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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.LogicalOperator;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.util.CollectionUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class ElasticSearchUtils {

    public static final String FIELD_ID = "_id";
    public static final String SYSTEM_PROPERTY_STRIP_INVALID_FIELD_CHARS = "metamodel.elasticsearch.strip_invalid_field_chars";

    public static QueryBuilder getMissingQuery(String fieldName) {
        return new BoolQueryBuilder().mustNot(new ExistsQueryBuilder(fieldName));
    }

    public static QueryBuilder getExistsQuery(String fieldName) {
        return new ExistsQueryBuilder(fieldName);
    }

    public static Map<String, ?> getMappingSource(final MutableTable table) {
        if (table.getColumnByName(FIELD_ID) == null) {
            final MutableColumn idColumn = new MutableColumn(FIELD_ID, ColumnType.STRING).setTable(table).setPrimaryKey(
                    true);
            table.addColumn(0, idColumn);
        }

        final Map<String, Map<String, String>> propertiesMap = new LinkedHashMap<>();
        
        for (Column column : table.getColumns()) {
            final String columnName = column.getName();
            if (FIELD_ID.equals(columnName)) {
                // do nothing - the ID is a client-side construct
                continue;
            }
            
            final String fieldName = getValidatedFieldName(columnName);
            final Map<String, String> propertyMap = new HashMap<>();
            final String type = getType(column);
            propertyMap.put("type", type);
            
            propertiesMap.put(fieldName, propertyMap);
        }

        HashMap<String, Map<String, Map<String, String>>> docTypeMap = new HashMap<>();
        docTypeMap.put("properties", propertiesMap);
        
        final Map<String, Map<String, Map<String, Map<String, String>>>> mapping = new HashMap<>();
        mapping.put(table.getName(), docTypeMap);
        return mapping;
    }

    /**
     * Field name special characters are:
     * 
     * . (used for navigation between name components)
     * 
     * # (for delimiting name components in _uid, should work, but is
     * discouraged)
     * 
     * * (for matching names)
     * 
     * @param fieldName
     * @return
     */
    public static String getValidatedFieldName(String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        if (fieldName.contains(".") || fieldName.contains("#") || fieldName.contains("*")) {
            if ("true".equalsIgnoreCase(System.getProperty(SYSTEM_PROPERTY_STRIP_INVALID_FIELD_CHARS, "true"))) {
                fieldName = fieldName.replace('.', '_').replace('#', '_').replace('*', '_');
            } else {
                throw new IllegalArgumentException("Field name '" + fieldName + "' contains illegal character (.#*)");
            }
        }
        return fieldName;
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
            return "text";
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

        throw new UnsupportedOperationException("Unsupported column type '" + type.getName() + "' of column '" + column
                .getName() + "' - cannot translate to an ElasticSearch type.");
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
                        itemQueryBuilder = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(fieldName,
                                operand));
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

    public static Row createRow(final Map<String, Object> sourceMap, final String documentId, final DataSetHeader header) {
        final Object[] values = new Object[header.size()];
        for (int i = 0; i < values.length; i++) {
            final SelectItem selectItem = header.getSelectItem(i);
            final Column column = selectItem.getColumn();

            assert column != null;
            assert selectItem.getAggregateFunction() == null;
            assert selectItem.getScalarFunction() == null;

            if (column.isPrimaryKey()) {
                values[i] = documentId;
            } else {
                Object value = sourceMap.get(column.getName());

                if (column.getType() == ColumnType.DATE) {
                    Date valueToDate = ElasticSearchDateConverter.tryToConvert((String) value);
                    if (valueToDate == null) {
                        values[i] = value;
                    } else {
                        values[i] = valueToDate;
                    }
                } else {
                    values[i] = value;
                }
            }
        }

        return new DefaultRow(header, values);
    }
}
