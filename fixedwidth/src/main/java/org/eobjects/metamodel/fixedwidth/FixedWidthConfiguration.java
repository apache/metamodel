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
