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
package org.eobjects.metamodel.fixedwidth;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.util.BaseObject;
import org.eobjects.metamodel.util.FileHelper;

/**
 * Configuration of metadata about a fixed width values datacontext.
 * 
 * @author Kasper Sørensen
 */
public final class FixedWidthConfiguration extends BaseObject implements
		Serializable {

	private static final long serialVersionUID = 1L;

	public static final int NO_COLUMN_NAME_LINE = 0;
	public static final int DEFAULT_COLUMN_NAME_LINE = 1;

	private final String encoding;
	private final int fixedValueWidth;
	private final int[] valueWidths;
	private final int columnNameLineNumber;
	private final boolean failOnInconsistentLineWidth;

	public FixedWidthConfiguration(int fixedValueWidth) {
		this(DEFAULT_COLUMN_NAME_LINE, FileHelper.DEFAULT_ENCODING,
				fixedValueWidth);
	}

	public FixedWidthConfiguration(int[] valueWidth) {
		this(DEFAULT_COLUMN_NAME_LINE, FileHelper.DEFAULT_ENCODING, valueWidth,
				false);
	}

	public FixedWidthConfiguration(int columnNameLineNumber, String encoding,
			int fixedValueWidth) {
		this(columnNameLineNumber, encoding, fixedValueWidth, false);
	}

	public FixedWidthConfiguration(int columnNameLineNumber, String encoding,
			int fixedValueWidth, boolean failOnInconsistentLineWidth) {
		this.encoding = encoding;
		this.fixedValueWidth = fixedValueWidth;
		this.columnNameLineNumber = columnNameLineNumber;
		this.failOnInconsistentLineWidth = failOnInconsistentLineWidth;
		this.valueWidths = new int[0];
	}

	public FixedWidthConfiguration(int columnNameLineNumber, String encoding,
			int[] valueWidths, boolean failOnInconsistentLineWidth) {
		this.encoding = encoding;
		this.fixedValueWidth = -1;
		this.columnNameLineNumber = columnNameLineNumber;
		this.failOnInconsistentLineWidth = failOnInconsistentLineWidth;
		this.valueWidths = valueWidths;
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
