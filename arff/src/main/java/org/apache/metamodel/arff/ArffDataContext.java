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
package org.apache.metamodel.arff;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.MaxRowsDataSet;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

/**
 * DataContext implementation for ARFF (.arff) files, which can be loosely described a specialized type of CSV files
 * with schema information, comments and data types in the header section of the file.
 */
public class ArffDataContext extends QueryPostprocessDataContext {

    private static final Logger logger = LoggerFactory.getLogger(ArffDataContext.class);

    private static final String SECTION_ANNOTATION_RELATION = "@relation";
    private static final String SECTION_ANNOTATION_ATTRIBUTE = "@attribute";
    private static final String SECTION_ANNOTATION_DATA = "@data";
    private static final Charset CHARSET = FileHelper.UTF_8_CHARSET;
    private static final Pattern ATTRIBUTE_DEF_W_DATATYPE_PARAM =
            Pattern.compile("\\'?(.+)\\'?\\s+([a-zA-Z]+)\\s+\\'(.+)\\'");

    private final Splitter whitespaceSplitter = Splitter.on(CharMatcher.whitespace()).trimResults().omitEmptyStrings();

    private final Resource resource;

    public ArffDataContext(Resource resource) {
        this.resource = resource;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final MutableSchema schema = new MutableSchema(getMainSchemaName());
        final MutableTable table = new MutableTable(getMainSchemaName(), TableType.TABLE, schema);
        schema.addTable(table);

        try (BufferedReader reader = createReader()) {
            boolean inHeader = true;
            for (String line = reader.readLine(); inHeader && line != null; line = reader.readLine()) {
                if (isIgnoreLine(line)) {
                    continue;
                }
                final List<String> split = whitespaceSplitter.limit(2).splitToList(line);
                switch (split.get(0).toLowerCase()) {
                case SECTION_ANNOTATION_RELATION:
                    // table name
                    final String tableName = trimString(split.get(1));
                    table.setName(tableName);
                    break;
                case SECTION_ANNOTATION_ATTRIBUTE:
                    // column(s)
                    final String attributeDef = split.get(1).trim();

                    final String attributeName;
                    final String attributeType;
                    final String attributeParam;
                    final ColumnType columnType;

                    final int indexOfCurly = attributeDef.indexOf('{');
                    if (indexOfCurly != -1) {
                        attributeName = trimString(attributeDef.substring(0, indexOfCurly).trim());
                        attributeType = attributeDef.substring(indexOfCurly);
                        attributeParam = null;
                    } else {
                        final Matcher matcher = ATTRIBUTE_DEF_W_DATATYPE_PARAM.matcher(attributeDef);
                        if (matcher.find()) {
                            attributeName = matcher.group(1);
                            attributeType = matcher.group(2);
                            attributeParam = matcher.group(3);
                        } else {
                            // simple attribute definition "[name] [type]"
                            final List<String> attributeDefSplit = whitespaceSplitter.splitToList(attributeDef);
                            if (attributeDefSplit.size() != 2) {
                                throw new IllegalStateException(
                                        resource.getName() + ": Unable to parse attribute line: " + line);
                            }
                            attributeName = trimString(attributeDefSplit.get(0));
                            attributeType = attributeDefSplit.get(1);
                            attributeParam = null;
                        }
                    }
                    switch (attributeType.toLowerCase()) {
                    case "numeric":
                        columnType = ColumnType.NUMBER;
                        break;
                    case "int":
                    case "integer":
                    case "short":
                        columnType = ColumnType.INTEGER;
                        break;
                    case "real":
                    case "double":
                    case "float":
                        columnType = ColumnType.DOUBLE;
                        break;
                    case "string":
                        columnType = ColumnType.STRING;
                        break;
                    case "date":
                        columnType = ColumnType.DATE;
                        break;
                    default:
                        if (indexOfCurly == -1) {
                            logger.info(
                                    "{}: Unrecognized data-type for attribute '{}': {}. Mapping to STRING column type.",
                                    resource.getName(), attributeName, attributeType);
                        }
                        columnType = ColumnType.STRING;
                        break;
                    }

                    final MutableColumn column = new MutableColumn(attributeName, columnType, table);
                    column.setRemarks(attributeParam == null ? attributeType : attributeType + " " + attributeParam);
                    column.setColumnNumber(table.getColumnCount());
                    table.addColumn(column);
                    break;
                case SECTION_ANNOTATION_DATA:
                    // the header part of the file is done, no more schema to build up
                    inHeader = false;
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return schema;
    }

    private String trimString(String string) {
        if (string.startsWith("'") && string.endsWith("'")) {
            string = string.substring(1, string.length() - 1);
        }
        return string;
    }

    private BufferedReader createReader() {
        return FileHelper.getBufferedReader(resource.read(), CHARSET);
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return resource.getName();
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
        BufferedReader reader = createReader();
        try {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                line = line.trim();
                if (isIgnoreLine(line)) {
                    continue;
                }
                if (line.toLowerCase().equals(SECTION_ANNOTATION_DATA)) {
                    // start of the data
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        final ArffDataSet dataSet = new ArffDataSet(resource, columns, reader);
        if (maxRows > -1) {
            return new MaxRowsDataSet(dataSet, maxRows);
        } else {
            return dataSet;
        }
    }

    protected static boolean isIgnoreLine(String line) {
        if (line.trim().isEmpty()) {
            return true;
        }
        if (line.startsWith("%")) {
            return true; // comment
        }
        return false;
    }

}
