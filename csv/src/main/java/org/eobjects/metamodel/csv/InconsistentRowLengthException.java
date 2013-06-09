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
package org.eobjects.metamodel.csv;

import org.eobjects.metamodel.InconsistentRowFormatException;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.Row;

/**
 * Exception thrown when a line in a CSV file has an inconsistent amount of
 * columns compared to the previous lines (and headers). The exception will be
 * thrown when {@link DataSet#next()} is called.
 * 
 * Note that this exception is only thrown if the
 * {@link CsvConfiguration#isFailOnInconsistentRowLength()} property is true.
 * Enabling it allows a somewhat different approach to iterating through a
 * resulting DataSet. For example something like:
 * 
 * <pre>
 * while (true) {
 * 	try {
 * 		if (!dataSet.next) {
 * 			break;
 * 		}
 * 		Row row = dataSet.getRow();
 * 		handleRegularRow(row);
 * 	} catch (InconsistentRowLengthException e) {
 * 		handleIrregularRow(e.getSourceLine());
 * 	}
 * }
 * </pre>
 * 
 * @author Kasper Sørensen
 */
public final class InconsistentRowLengthException extends
		InconsistentRowFormatException {

	private static final long serialVersionUID = 1L;

	private final int _columnsInTable;
	private final String[] _line;

	public InconsistentRowLengthException(int columnsInTable, Row proposedRow,
			String[] line, int rowNumber) {
		super(proposedRow, rowNumber);
		_columnsInTable = columnsInTable;
		_line = line;
	}

	@Override
	public String getMessage() {
		return "Inconsistent length of row no. " + getRowNumber()
				+ ". Expected " + getColumnsInTable() + " columns but found "
				+ getColumnsInLine() + ".";
	}

	/**
	 * Gets the source line, as parsed by the CSV parser (regardless of table
	 * metadata).
	 * 
	 * @return an array of string values.
	 */
	public String[] getSourceLine() {
		return _line;
	}

	/**
	 * Gets the amount of columns in the parsed line.
	 * 
	 * @return an int representing the amount of values in the inconsistent
	 *         line.
	 */
	public int getColumnsInLine() {
		return _line.length;
	}

	/**
	 * Gets the expected amounts of columns, as defined by the table metadata.
	 * 
	 * @return an int representing the amount of columns defined in the table.
	 */
	public int getColumnsInTable() {
		return _columnsInTable;
	}
}
