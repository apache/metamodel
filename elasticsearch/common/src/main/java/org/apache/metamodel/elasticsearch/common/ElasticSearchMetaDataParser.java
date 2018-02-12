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

import java.util.Map;
import java.util.Map.Entry;

import org.apache.metamodel.schema.ColumnType;

/**
 * Parser that transforms the ElasticSearch metadata response (json-like format)
 * into an ElasticSearchMetaData object.
 */
public class ElasticSearchMetaDataParser {

    /**
     * Parses the ElasticSearch meta data info into an ElasticSearchMetaData
     * object. This method makes much easier to create the ElasticSearch schema.
     *
     * @param metaDataInfo
     *            ElasticSearch mapping metadata in Map format
     * @return An ElasticSearchMetaData object
     */
    public static ElasticSearchMetaData parse(Map<String, ?> metaDataInfo) {
        final String[] fieldNames = new String[metaDataInfo.size() + 1];
        final ColumnType[] columnTypes = new ColumnType[metaDataInfo.size() + 1];

        // add the document ID field (fixed)
        fieldNames[0] = ElasticSearchUtils.FIELD_ID;
        columnTypes[0] = ColumnType.STRING;

        int i = 1;
        for (Entry<String, ?> metaDataField : metaDataInfo.entrySet()) {
            @SuppressWarnings("unchecked")
            final Map<String, ?> fieldMetadata = (Map<String, ?>) metaDataField.getValue();

            fieldNames[i] = metaDataField.getKey();
            columnTypes[i] = getColumnTypeFromMetadataField(fieldMetadata);
            i++;

        }
        return new ElasticSearchMetaData(fieldNames, columnTypes);
    }

    private static ColumnType getColumnTypeFromMetadataField(Map<String, ?> fieldMetadata) {
        final String metaDataFieldType = getMetaDataFieldTypeFromMetaDataField(fieldMetadata);

        if (metaDataFieldType == null) {
            return ColumnType.STRING;
        }

        return ElasticSearchUtils.getColumnTypeFromElasticSearchType(metaDataFieldType);
    }

    private static String getMetaDataFieldTypeFromMetaDataField(Map<String, ?> metaDataField) {
        final Object type = metaDataField.get("type");
        if (type == null) {
            return null;
        }
        return type.toString();
    }

}
