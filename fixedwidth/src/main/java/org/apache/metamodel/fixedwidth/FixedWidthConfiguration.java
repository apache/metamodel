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
package org.apache.metamodel.fixedwidth;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.naming.ColumnNamingStrategies;
import org.apache.metamodel.schema.naming.ColumnNamingStrategy;
import org.apache.metamodel.util.BaseObject;
import org.apache.metamodel.util.CollectionUtils;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.HasNameMapper;

/**
 * Configuration of metadata about a fixed width values data context.
 */
public class FixedWidthConfiguration extends BaseObject implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int NO_COLUMN_NAME_LINE = 0;
    public static final int DEFAULT_COLUMN_NAME_LINE = 1;

    private final String encoding;
    private final int fixedValueWidth;
    private final int[] valueWidths;
    private final int columnNameLineNumber;
    private final boolean failOnInconsistentLineWidth;
    private final ColumnNamingStrategy columnNamingStrategy;

    public FixedWidthConfiguration(int fixedValueWidth) {
        this(DEFAULT_COLUMN_NAME_LINE, FileHelper.DEFAULT_ENCODING, fixedValueWidth);
    }

    public FixedWidthConfiguration(int[] valueWidth) {
        this(DEFAULT_COLUMN_NAME_LINE, FileHelper.DEFAULT_ENCODING, valueWidth, false);
    }

    public FixedWidthConfiguration(int columnNameLineNumber, String encoding, int fixedValueWidth) {
        this(columnNameLineNumber, encoding, fixedValueWidth, false);
    }

    public FixedWidthConfiguration(int columnNameLineNumber, String encoding, int fixedValueWidth,
            boolean failOnInconsistentLineWidth) {
        this.encoding = encoding;
        this.fixedValueWidth = fixedValueWidth;
        this.columnNameLineNumber = columnNameLineNumber;
        this.failOnInconsistentLineWidth = failOnInconsistentLineWidth;
        this.columnNamingStrategy = null;
        this.valueWidths = new int[0];
    }

    public FixedWidthConfiguration(int columnNameLineNumber, String encoding, int[] valueWidths, 
            boolean failOnInconsistentLineWidth) {
        this(columnNameLineNumber, null, encoding, valueWidths, failOnInconsistentLineWidth);
    }

    public FixedWidthConfiguration(int columnNameLineNumber, ColumnNamingStrategy columnNamingStrategy, String encoding,
            int[] valueWidths, boolean failOnInconsistentLineWidth) {
        this.encoding = encoding;
        this.fixedValueWidth = -1;
        this.columnNameLineNumber = columnNameLineNumber;
        this.failOnInconsistentLineWidth = failOnInconsistentLineWidth;
        this.columnNamingStrategy = columnNamingStrategy;
        this.valueWidths = valueWidths;
    }

    public FixedWidthConfiguration(String encoding, List<FixedWidthColumnSpec> columnSpecs) {
        this(encoding, columnSpecs, false);
    }

    public FixedWidthConfiguration(String encoding, List<FixedWidthColumnSpec> columnSpecs,
            boolean failOnInconsistentLineWidth) {
        this.encoding = encoding;
        this.fixedValueWidth = -1;
        this.columnNameLineNumber = NO_COLUMN_NAME_LINE;
        this.columnNamingStrategy = ColumnNamingStrategies.customNames(CollectionUtils.map(columnSpecs,
                new HasNameMapper()));
        this.valueWidths = new int[columnSpecs.size()];
        for (int i = 0; i < valueWidths.length; i++) {
            valueWidths[i] = columnSpecs.get(i).getWidth();
        }
        this.failOnInconsistentLineWidth = failOnInconsistentLineWidth;
    }

    /**
     * The line number (1 based) from which to get the names of the columns.
     *
     * @return an int representing the line number of the column headers/names.
     */
    public int getColumnNameLineNumber() {
        return columnNameLineNumber;
    }

    /**
     * Gets a {@link ColumnNamingStrategy} to use if needed.
     * @return column naming strategy
     */
    public ColumnNamingStrategy getColumnNamingStrategy() {
        if (columnNamingStrategy == null) {
            return ColumnNamingStrategies.defaultStrategy();
        }
        return columnNamingStrategy;
    }

    /**
     * Gets the file encoding to use for reading the file.
     *
     * @return the text encoding to use for reading the file.
     */
    public String getEncoding() {
        return encoding;
    }

    /**
     * Gets the width of each value within the fixed width value file.
     *
     * @return the fixed width to use when parsing the file.
     */
    public int getFixedValueWidth() {
        return fixedValueWidth;
    }

    public int[] getValueWidths() {
        return valueWidths;
    }

    /**
     * Determines if the {@link DataSet#next()} should throw an exception in
     * case of inconsistent line width in the fixed width value file.
     *
     * @return a boolean indicating whether or not to fail on inconsistent line
     *         widths.
     */
    public boolean isFailOnInconsistentLineWidth() {
        return failOnInconsistentLineWidth;
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(columnNameLineNumber);
        identifiers.add(encoding);
        identifiers.add(fixedValueWidth);
        identifiers.add(valueWidths);
        identifiers.add(failOnInconsistentLineWidth);
    }

    @Override
    public String toString() {
        return "FixedWidthConfiguration[encoding=" + encoding
                + ", fixedValueWidth=" + fixedValueWidth + ", valueWidths="
                + Arrays.toString(valueWidths) + ", columnNameLineNumber="
                + columnNameLineNumber + ", failOnInconsistentLineWidth="
                + failOnInconsistentLineWidth + "]";
    }

    public boolean isConstantValueWidth() {
        return fixedValueWidth != -1;
    }

    public int getValueWidth(int columnIndex) {
        if (isConstantValueWidth()) {
            return fixedValueWidth;
        }
        return valueWidths[columnIndex];
    }
}
