/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel.excel;

import java.io.Serializable;
import java.util.List;

import org.eobjects.metamodel.util.BaseObject;

/**
 * Represents the configuration for reading/parsing Excel spreadsheets.
 * 
 * @see ExcelDataContext
 * 
 * @author Kasper Sørensen
 */
public final class ExcelConfiguration extends BaseObject implements
		Serializable {

	private static final long serialVersionUID = 1L;

	public static final int NO_COLUMN_NAME_LINE = 0;
	public static final int DEFAULT_COLUMN_NAME_LINE = 1;

	private final int columnNameLineNumber;
	private final boolean skipEmptyLines;
	private final boolean skipEmptyColumns;

	public ExcelConfiguration() {
		this(DEFAULT_COLUMN_NAME_LINE, true, false);
	}

	public ExcelConfiguration(int columnNameLineNumber, boolean skipEmptyLines,
			boolean skipEmptyColumns) {
		this.columnNameLineNumber = columnNameLineNumber;
		this.skipEmptyLines = skipEmptyLines;
		this.skipEmptyColumns = skipEmptyColumns;
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
