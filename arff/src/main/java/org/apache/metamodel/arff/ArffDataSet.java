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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.util.NumberComparator;
import org.apache.metamodel.util.Resource;

import com.opencsv.CSVParser;
import com.opencsv.ICSVParser;

final class ArffDataSet extends AbstractDataSet {

    private final ICSVParser csvParser = new CSVParser(',', '\'');
    private final Resource resource;
    private final BufferedReader reader;
    private final List<Column> columns;

    private String line;

    public ArffDataSet(Resource resource, List<Column> columns, BufferedReader reader) {
        super(columns.stream().map(c -> new SelectItem(c)).collect(Collectors.toList()));
        this.resource = resource;
        this.columns = columns;
        this.reader = reader;
    }

    @Override
    public boolean next() {
        try {
            line = reader.readLine();
            while (line != null && ArffDataContext.isIgnoreLine(line)) {
                line = reader.readLine();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return line != null;
    }

    @Override
    public Row getRow() {
        if (line == null) {
            return null;
        }
        final String[] stringValues;
        try {
            stringValues = csvParser.parseLine(line);
        } catch (IOException e) {
            throw new UncheckedIOException(resource.getName() + ": Failed to CSV-parse data line: " + line, e);
        }

        final Object[] values = new Object[columns.size()];
        for (int i = 0; i < values.length; i++) {
            final Column column = columns.get(i);
            final int index = column.getColumnNumber();
            final String stringValue = stringValues[index];
            values[i] = convertValue(stringValue, column);
        }
        return new DefaultRow(getHeader(), values);
    }

    private Object convertValue(String stringValue, Column column) {
        final ColumnType type = column.getType();
        if (type.isNumber()) {
            if (stringValue.isEmpty() || "?".equals(stringValue)) {
                return null;
            } else {
                final Number n = NumberComparator.toNumber(stringValue);
                if (type == ColumnType.INTEGER) {
                    return n.intValue();
                } else {
                    return n;
                }
            }
        } else if (type.isTimeBased()) {
            final String columnRemarks = column.getRemarks();
            final SimpleDateFormat dateFormat;
            if (columnRemarks.toLowerCase().startsWith("date ")) {
                // date format follows "date "
                dateFormat = new SimpleDateFormat(columnRemarks.substring(5));
            } else {
                // assume standard date format
                dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }
            try {
                return dateFormat.parse(stringValue);
            } catch (ParseException e) {
                throw new IllegalStateException(resource.getName() + ": Failed to parse '" + stringValue
                        + "' using format '" + dateFormat.toPattern() + "'", e);
            }
        } else {
            return stringValue;
        }
    }

    @Override
    public void close() {
        super.close();
    }
}
