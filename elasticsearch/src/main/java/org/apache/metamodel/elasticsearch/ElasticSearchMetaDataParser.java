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

public class ElasticSearchMetaDataParser {

    public static ElasticSearchMetaData parse(String metaDataInfo) {
        String plainMetaDataInfo = removeFirstAndLastCharacter(metaDataInfo);
        String metaDataWithoutDateFormats = removeDateFormats(plainMetaDataInfo);
        String[] metaDataFields = metaDataWithoutDateFormats.split(",");
        String[] fieldNames = new String[metaDataFields.length];
        ColumnType[] columnTypes = new ColumnType[metaDataFields.length];
        int i = 0;
        for (String metaDataField: metaDataFields) {
            //message={type=long}
            fieldNames[i] = getNameFromMetaDataField(metaDataField);
            columnTypes[i] = getColumnTypeFromMetaDataField(metaDataField);
            i++;

        }
        return new ElasticSearchMetaData(fieldNames, columnTypes);
    }

    private static String removeFirstAndLastCharacter(String metaDataInfo) {
        return metaDataInfo.substring(1, metaDataInfo.length()-1);
    }

    private static String removeDateFormats(String metaDataInfo) {
        return metaDataInfo.replaceAll("type=date.*?}", "type=date}");
    }

    private static String getNameFromMetaDataField(String metaDataField) {
        return metaDataField.substring(0, metaDataField.indexOf("=")).trim();
    }

    private static ColumnType getColumnTypeFromMetaDataField(String metaDataField) {
        ColumnType columnType = null;
        String metaDataFieldType = getMetaDataFieldTypeFromMetaDataField(metaDataField);
        switch (metaDataFieldType) {
            case "long" : columnType = ColumnType.BIGINT;
                            break;
            case "date" : columnType = ColumnType.DATE;
                break;
            case "string" : columnType = ColumnType.STRING;
                break;
            case "float" : columnType = ColumnType.FLOAT;
                break;
            case "boolean" : columnType = ColumnType.BOOLEAN;
                break;
            case "default" : return ColumnType.VARCHAR;
        }
        return columnType;
    }

    private static String getMetaDataFieldTypeFromMetaDataField(String metaDataField) {
        String metaDataFieldWithoutName = metaDataField.substring(metaDataField.indexOf("=")+1);
        String metaDataFieldType = metaDataFieldWithoutName.substring(metaDataFieldWithoutName.indexOf("=")+1, metaDataFieldWithoutName.length()-1);
        return metaDataFieldType;
    }

}
