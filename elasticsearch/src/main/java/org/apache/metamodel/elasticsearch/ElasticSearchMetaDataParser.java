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
package org.apache.metamodel.elasticsearch;

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
     *            ElasticSearch metadata info in a json-like format
     * @return An ElasticSearchMetaData object
     */
    public static ElasticSearchMetaData parse(Object metaDataInfo) {
        final String plainMetaDataInfo = removeFirstAndLastCharacter(metaDataInfo.toString());
        final String metaDataWithoutDateFormats = removeDateFormats(plainMetaDataInfo);
        final String[] metaDataFields = metaDataWithoutDateFormats.split("},");

        final String[] fieldNames = new String[metaDataFields.length + 1];
        final ColumnType[] columnTypes = new ColumnType[metaDataFields.length + 1];

        // add the document ID field (fixed)
        fieldNames[0] = ElasticSearchDataContext.FIELD_ID;
        columnTypes[0] = ColumnType.STRING;

        int i = 1;
        for (String metaDataField : metaDataFields) {
            // message={type=long}
            fieldNames[i] = getNameFromMetaDataField(metaDataField);
            columnTypes[i] = getColumnTypeFromMetaDataField(metaDataField);
            i++;

        }
        return new ElasticSearchMetaData(fieldNames, columnTypes);
    }

    private static String removeFirstAndLastCharacter(String metaDataInfo) {
        return metaDataInfo.substring(1, metaDataInfo.length() - 1);
    }

    private static String removeDateFormats(String metaDataInfo) {
        return metaDataInfo.replaceAll("type=date.*?}", "type=date}");
    }

    private static String getNameFromMetaDataField(String metaDataField) {
        return metaDataField.substring(0, metaDataField.indexOf("=")).trim();
    }

    private static ColumnType getColumnTypeFromMetaDataField(String metaDataField) {
        final ColumnType columnType;
        final String metaDataFieldType = getMetaDataFieldTypeFromMetaDataField(metaDataField);
        if (metaDataFieldType.equals("long")) {
            columnType = ColumnType.BIGINT;
        } else if (metaDataFieldType.equals("date")) {
            columnType = ColumnType.DATE;
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

    private static String getMetaDataFieldTypeFromMetaDataField(String metaDataField) {
        String type = metaDataField.substring(metaDataField.indexOf("type=") + 5);
        if (type.indexOf(",") > 0) {
            type = type.substring(0, type.indexOf(","));
        }
        if (type.indexOf("}") > 0) {
            type = type.substring(0, type.indexOf("}"));
        }
        return type;
    }

}
