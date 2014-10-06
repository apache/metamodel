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

import org.apache.metamodel.InconsistentRowFormatException;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;

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
