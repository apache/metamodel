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
package org.eobjects.metamodel;

import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.Row;

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
 * 
 * @author Kasper Sørensen
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
