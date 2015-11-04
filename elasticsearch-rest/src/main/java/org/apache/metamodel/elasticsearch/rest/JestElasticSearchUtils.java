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
package org.apache.metamodel.elasticsearch.rest;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.util.NumberComparator;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Date;

/**
 * Shared/common util functions for the ElasticSearch MetaModel module.
 */
final class JestElasticSearchUtils {
    private static final Logger logger = LoggerFactory.getLogger(JestElasticSearchUtils.class);

    public static Row createRow(JsonObject source, String documentId, DataSetHeader header) {
        final Object[] values = new Object[header.size()];
        for (int i = 0; i < values.length; i++) {
            final SelectItem selectItem = header.getSelectItem(i);
            final Column column = selectItem.getColumn();

            assert column != null;
            assert !selectItem.hasFunction();

            if (column.isPrimaryKey()) {
                values[i] = documentId;
            } else {
                values[i] = getDataFromColumnType(source.get(column.getName()), column.getType());
            }
        }

        return new DefaultRow(header, values);
    }

    private static Object getDataFromColumnType(JsonElement field, ColumnType type) {
        if (type.isNumber()) {
            // Pretty terrible workaround to avoid LazilyParsedNumber
            // (which is happily output, but not recognized by Jest/GSON).
            return NumberComparator.toNumber(field.getAsString());
        } else if (type.isTimeBased()) {
            Date valueToDate = JestElasticSearchDateConverter.tryToConvert(field.getAsString());
            if (valueToDate == null) {
                return field.getAsString();
            } else {
                return valueToDate;
            }
        } else if (type.isBoolean()) {
            return field.getAsBoolean();
        } else {
            return field.getAsString();
        }
    }

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
                Class<?> clazz = JestElasticSearchUtils.class.getClassLoader()
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

}
