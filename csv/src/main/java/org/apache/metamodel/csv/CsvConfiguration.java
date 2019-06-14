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
package org.apache.metamodel.csv;

import java.io.Serializable;
import java.util.List;

import org.apache.metamodel.schema.naming.ColumnNamingStrategies;
import org.apache.metamodel.schema.naming.ColumnNamingStrategy;
import org.apache.metamodel.util.BaseObject;
import org.apache.metamodel.util.FileHelper;

/**
 * Represents the configuration for reading/parsing CSV files.
 */
public final class CsvConfiguration extends BaseObject implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The value is '\\uFFFF', the "not a character" value which should not
     * occur in any valid Unicode string. This special char can be used to
     * disable either quote chars or escape chars.
     */
    public static final char NOT_A_CHAR = '\uFFFF';
    public static final int NO_COLUMN_NAME_LINE = 0;
    public static final int DEFAULT_COLUMN_NAME_LINE = 1;
    public static final char DEFAULT_SEPARATOR_CHAR = ',';
    public static final char DEFAULT_QUOTE_CHAR = '"';
    public static final char DEFAULT_ESCAPE_CHAR = '\\';

    private final int columnNameLineNumber;
    private final String encoding;
    private final char separatorChar;
    private final char quoteChar;
    private final char escapeChar;
    private final boolean failOnInconsistentRowLength;
    private final boolean multilineValues;
    private final ColumnNamingStrategy columnNamingStrategy;

    public CsvConfiguration() {
        this(DEFAULT_COLUMN_NAME_LINE);
    }

    public CsvConfiguration(int columnNameLineNumber) {
        this(columnNameLineNumber, FileHelper.DEFAULT_ENCODING, DEFAULT_SEPARATOR_CHAR, DEFAULT_QUOTE_CHAR,
                DEFAULT_ESCAPE_CHAR);
    }

    public CsvConfiguration(int columnNameLineNumber, boolean failOnInconsistentRowLength, boolean multilineValues) {
        this(columnNameLineNumber, FileHelper.DEFAULT_ENCODING, DEFAULT_SEPARATOR_CHAR, DEFAULT_QUOTE_CHAR,
                DEFAULT_ESCAPE_CHAR, failOnInconsistentRowLength, multilineValues);
    }

    public CsvConfiguration(int columnNameLineNumber, String encoding, char separatorChar, char quoteChar,
            char escapeChar) {
        this(columnNameLineNumber, encoding, separatorChar, quoteChar, escapeChar, false);
    }

    public CsvConfiguration(int columnNameLineNumber, String encoding, char separatorChar, char quoteChar,
            char escapeChar, boolean failOnInconsistentRowLength) {
        this(columnNameLineNumber, encoding, separatorChar, quoteChar, escapeChar, failOnInconsistentRowLength, true);
    }
    
    public CsvConfiguration(int columnNameLineNumber, String encoding, char separatorChar, char quoteChar,
            char escapeChar, boolean failOnInconsistentRowLength, boolean multilineValues) {
        this(columnNameLineNumber, null, encoding, separatorChar, quoteChar, escapeChar, failOnInconsistentRowLength,
                multilineValues);
    }

    public CsvConfiguration(int columnNameLineNumber, ColumnNamingStrategy columnNamingStrategy, String encoding,
            char separatorChar, char quoteChar, char escapeChar, boolean failOnInconsistentRowLength,
            boolean multilineValues) {
        this.columnNameLineNumber = columnNameLineNumber;
        this.encoding = encoding;
        this.separatorChar = separatorChar;
        this.quoteChar = quoteChar;
        this.escapeChar = escapeChar;
        this.failOnInconsistentRowLength = failOnInconsistentRowLength;
        this.multilineValues = multilineValues;
        this.columnNamingStrategy = columnNamingStrategy;
    }
    
    /**
     * Gets a {@link ColumnNamingStrategy} to use if needed.
     * @return
     */
    public ColumnNamingStrategy getColumnNamingStrategy() {
        if (columnNamingStrategy == null) {
            return ColumnNamingStrategies.defaultStrategy();
        }
        return columnNamingStrategy;
    }

    /**
     * Determines whether to fail (by throwing an
     * {@link InconsistentRowLengthException}) if a line in the CSV file has
     * inconsistent amounts of columns.
     * 
     * If set to false (default) MetaModel will gracefully fill in missing null
     * values in or ignore additional values in a line.
     * 
     * @return a boolean indicating whether to fail or gracefully compensate for
     *         inconsistent lines in the CSV files.
     */
    public boolean isFailOnInconsistentRowLength() {
        return failOnInconsistentRowLength;
    }

    /**
     * Determines whether the CSV files read using this configuration should be
     * allowed to have multiline values in them.
     * 
     * @return
     */
    public boolean isMultilineValues() {
        return multilineValues;
    }

    /**
     * The line number (1 based) from which to get the names of the columns.
     * 
     * @return the line number (1 based)
     */
    public int getColumnNameLineNumber() {
        return columnNameLineNumber;
    }

    /**
     * Gets the file encoding to use for reading the file.
     * 
     * @return the text encoding of the file.
     */
    public String getEncoding() {
        return encoding;
    }

    /**
     * Gets the separator char (typically comma or semicolon) for separating
     * values.
     * 
     * @return the separator char
     */
    public char getSeparatorChar() {
        return separatorChar;
    }

    /**
     * Gets the quote char, used for encapsulating values.
     * 
     * @return the quote char
     */
    public char getQuoteChar() {
        return quoteChar;
    }

    /**
     * Gets the escape char, used for escaping eg. quote chars inside values.
     * 
     * @return the escape char
     */
    public char getEscapeChar() {
        return escapeChar;
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(columnNameLineNumber);
        identifiers.add(encoding);
        identifiers.add(separatorChar);
        identifiers.add(quoteChar);
        identifiers.add(escapeChar);
        identifiers.add(failOnInconsistentRowLength);
    }

    @Override
    public String toString() {
        return "CsvConfiguration[columnNameLineNumber=" + columnNameLineNumber + ", encoding=" + encoding
                + ", separatorChar=" + separatorChar + ", quoteChar=" + quoteChar + ", escapeChar=" + escapeChar
                + ", failOnInconsistentRowLength=" + failOnInconsistentRowLength + "]";
    }
}
