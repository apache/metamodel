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
package org.apache.metamodel;

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;

/**
 * Abstract exception type that represents exceptions that occur when reading a
 * data format which contain formatting errors or inconsistencies in on or more
 * rows.
 * 
 * Typically {@link InconsistentRowFormatException}s are thrown when calling
 * {@link DataSet#next()}.
 * 
 * All {@link InconsistentRowFormatException}s are optional, meaning that you
 * can turn them on and off. When turned off the result of
 * {@link #getProposedRow()} will be used transparently instead of throwing the
 * exception.
 */
public abstract class InconsistentRowFormatException extends MetaModelException {

	private static final long serialVersionUID = 1L;

	private final Row _proposedRow;
	private final int _rowNumber;

	public InconsistentRowFormatException(Row proposedRow, int rowNumber) {
		super();
		_proposedRow = proposedRow;
		_rowNumber = rowNumber;
	}

	public InconsistentRowFormatException(Row proposedRow, int rowNumber,
			Exception cause) {
		super(cause);
		_proposedRow = proposedRow;
		_rowNumber = rowNumber;
	}

	/**
	 * Gets the row as MetaModel would gracefully interpret it.
	 * 
	 * @return a row object which represents the {@link Row} as MetaModel would
	 *         gracefully interpret it.
	 */
	public Row getProposedRow() {
		return _proposedRow;
	}

	/**
	 * Gets the row number (1-based).
	 * 
	 * @return the index of the row.
	 */
	public int getRowNumber() {
		return _rowNumber;
	}

	@Override
	public String getMessage() {
		return "Inconsistent row format of row no. " + getRowNumber() + ".";
	}
}
