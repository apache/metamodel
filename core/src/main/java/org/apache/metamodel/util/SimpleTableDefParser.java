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
package org.apache.metamodel.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.ColumnTypeImpl;

public class SimpleTableDefParser {

    /**
     * Parses an array of table definitions.
     * 
     * @param tableDefinitionsText
     * @return
     */
    public static SimpleTableDef[] parseTableDefs(String tableDefinitionsText) {
        if (tableDefinitionsText == null) {
            return null;
        }

        tableDefinitionsText = tableDefinitionsText.replaceAll("\n", "").replaceAll("\t", "").replaceAll("\r", "")
                .replaceAll("  ", " ");

        final List<SimpleTableDef> tableDefs = new ArrayList<SimpleTableDef>();
        final String[] tableDefinitionTexts = tableDefinitionsText.split(";");
        for (String tableDefinitionText : tableDefinitionTexts) {
            final SimpleTableDef tableDef = parseTableDef(tableDefinitionText);
            if (tableDef != null) {
                tableDefs.add(tableDef);
            }
        }

        if (tableDefs.isEmpty()) {
            return null;
        }

        return tableDefs.toArray(new SimpleTableDef[tableDefs.size()]);
    }

    /**
     * Parses a single table definition
     * 
     * @param tableDefinitionText
     * @return
     */
    protected static SimpleTableDef parseTableDef(String tableDefinitionText) {
        if (tableDefinitionText == null || tableDefinitionText.trim().isEmpty()) {
            return null;
        }

        int startColumnSection = tableDefinitionText.indexOf("(");
        if (startColumnSection == -1) {
            throw new IllegalArgumentException("Failed to parse table definition: " + tableDefinitionText
                    + ". No start parenthesis found for column section.");
        }

        int endColumnSection = tableDefinitionText.indexOf(")", startColumnSection);
        if (endColumnSection == -1) {
            throw new IllegalArgumentException("Failed to parse table definition: " + tableDefinitionText
                    + ". No end parenthesis found for column section.");
        }

        String tableName = tableDefinitionText.substring(0, startColumnSection).trim();
        if (tableName.isEmpty()) {
            throw new IllegalArgumentException("Failed to parse table definition: " + tableDefinitionText
                    + ". No table name found.");
        }

        String columnSection = tableDefinitionText.substring(startColumnSection + 1, endColumnSection);

        String[] columnDefinitionTexts = columnSection.split(",");
        List<String> columnNames = new ArrayList<String>();
        List<ColumnType> columnTypes = new ArrayList<ColumnType>();

        for (String columnDefinition : columnDefinitionTexts) {
            columnDefinition = columnDefinition.trim();
            if (!columnDefinition.isEmpty()) {
                int separator = columnDefinition.lastIndexOf(" ");
                String columnName = columnDefinition.substring(0, separator).trim();
                String columnTypeString = columnDefinition.substring(separator).trim();
                ColumnType columnType = ColumnTypeImpl.valueOf(columnTypeString);

                columnNames.add(columnName);
                columnTypes.add(columnType);
            }
        }
        return new SimpleTableDef(tableName, columnNames.toArray(new String[columnNames.size()]),
                columnTypes.toArray(new ColumnType[columnTypes.size()]));
    }
}
