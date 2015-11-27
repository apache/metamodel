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

import org.apache.metamodel.elasticsearch.common.ElasticSearchMetaData;
import org.apache.metamodel.elasticsearch.common.ElasticSearchUtils;
import org.apache.metamodel.schema.ColumnType;

import java.util.Map.Entry;

/**
 * Parser that transforms the ElasticSearch metadata response (json-like format)
 * into an ElasticSearchMetaData object.
 */
final class JestElasticSearchMetaDataParser {

    /**
     * Parses the ElasticSearch meta data info into an ElasticSearchMetaData
     * object. This method makes much easier to create the ElasticSearch schema.
     *
     * @param metaDataInfo
     *            ElasticSearch mapping metadata in Map format
     * @return An ElasticSearchMetaData object
     */
    public static ElasticSearchMetaData parse(JsonObject metaDataInfo) {
        final int columns = metaDataInfo.entrySet().size() + 1;
        final String[] fieldNames = new String[columns];
        final ColumnType[] columnTypes = new ColumnType[columns];

        // add the document ID field (fixed)
        fieldNames[0] = ElasticSearchRestDataContext.FIELD_ID;
        columnTypes[0] = ColumnType.STRING;

        int i = 1;
        for (Entry<String, JsonElement> metaDataField : metaDataInfo.entrySet()) {
            JsonElement fieldMetadata = metaDataField.getValue();

            fieldNames[i] = metaDataField.getKey();
            columnTypes[i] = getColumnTypeFromMetadataField(fieldMetadata);
            i++;

        }
        return new ElasticSearchMetaData(fieldNames, columnTypes);
    }

    private static ColumnType getColumnTypeFromMetadataField(JsonElement fieldMetadata) {
        final JsonElement typeElement = ((JsonObject) fieldMetadata).get("type");
        if (typeElement != null) {
            String metaDataFieldType = typeElement.getAsString();

            return ElasticSearchUtils.getColumnTypeFromElasticSearchType(metaDataFieldType);
        } else {
            return ColumnType.STRING;
        }
    }
}
