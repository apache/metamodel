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
package org.apache.metamodel.excel;

import java.io.Serializable;
import java.util.List;

import org.apache.metamodel.schema.naming.ColumnNamingStrategies;
import org.apache.metamodel.schema.naming.ColumnNamingStrategy;
import org.apache.metamodel.util.BaseObject;

/**
 * Represents the configuration for reading/parsing Excel spreadsheets.
 * 
 * @see ExcelDataContext
 */
public final class ExcelConfiguration extends BaseObject implements
		Serializable {

	private static final long serialVersionUID = 1L;

	public static final int NO_COLUMN_NAME_LINE = 0;
	public static final int DEFAULT_COLUMN_NAME_LINE = 1;

	private final int columnNameLineNumber;
	private final ColumnNamingStrategy columnNamingStrategy;
	private final boolean skipEmptyLines;
	private final boolean skipEmptyColumns;

	public ExcelConfiguration() {
		this(DEFAULT_COLUMN_NAME_LINE, true, false);
	}

    public ExcelConfiguration(int columnNameLineNumber, boolean skipEmptyLines, boolean skipEmptyColumns) {
        this(columnNameLineNumber, null, skipEmptyLines, skipEmptyColumns);
    }

    public ExcelConfiguration(int columnNameLineNumber, ColumnNamingStrategy columnNamingStrategy,
            boolean skipEmptyLines, boolean skipEmptyColumns) {
        this.columnNameLineNumber = columnNameLineNumber;
        this.skipEmptyLines = skipEmptyLines;
        this.skipEmptyColumns = skipEmptyColumns;
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
	 * The line number (1 based) from which to get the names of the columns.
	 * Note that this line number is affected by the skipEmptyLines property! If
	 * skipEmptyLines is set to true, the line numbers will begin from the first
	 * non-empty line.
	 * 
	 * @return the line number of the column headers/names.
	 */
	public int getColumnNameLineNumber() {
		return columnNameLineNumber;
	}

	/**
	 * Defines if empty lines in the excel spreadsheet should be skipped while
	 * reading the spreadsheet.
	 * 
	 * @return a boolean indicating whether or not to skip empty lines.
	 */
	public boolean isSkipEmptyLines() {
		return skipEmptyLines;
	}

	/**
	 * Defines if empty columns in the excel spreadsheet should be skipped while
	 * reading the spreadsheet.
	 * 
	 * @return a boolean indicating whether or not to skip empty columns.
	 */
	public boolean isSkipEmptyColumns() {
		return skipEmptyColumns;
	}

	@Override
	protected void decorateIdentity(List<Object> identifiers) {
		identifiers.add(columnNameLineNumber);
		identifiers.add(skipEmptyLines);
		identifiers.add(skipEmptyColumns);
	}

	@Override
	public String toString() {
		return "ExcelConfiguration[columnNameLineNumber="
				+ columnNameLineNumber + ", skipEmptyLines=" + skipEmptyLines
				+ ", skipEmptyColumns=" + skipEmptyColumns + "]";
	}
}
