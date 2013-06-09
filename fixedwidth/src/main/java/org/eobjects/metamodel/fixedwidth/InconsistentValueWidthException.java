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

import org.eobjects.metamodel.InconsistentRowFormatException;
import org.eobjects.metamodel.data.Row;

/**
 * Exception thrown when inconsistent widths of a Fixed Width Value file
 * 
 * @author Kasper Sørensen
 */
public final class InconsistentValueWidthException extends
		InconsistentRowFormatException {

	private static final long serialVersionUID = 1L;
	private final String[] _sourceResult;
	private final String _sourceLine;

	public InconsistentValueWidthException(String[] result, String line,
			int rowNumber) {
		super(null, rowNumber);
		_sourceResult = result;
		_sourceLine = line;
	}

	public InconsistentValueWidthException(Row proposedRow,
			InconsistentValueWidthException cause) {
		super(proposedRow, cause.getRowNumber(), cause);
		_sourceResult = cause.getSourceResult();
		_sourceLine = cause.getSourceLine();
	}

	/**
	 * Gets the source line as represented in the Fixed Width file
	 * 
	 * @return the source line as a string
	 */
	public String getSourceLine() {
		return _sourceLine;
	}

	/**
	 * Gets the parsed result as read by the Fixed Width reader.
	 * 
	 * @return the gracefully parsed line
	 */
	public String[] getSourceResult() {
		return _sourceResult;
	}
}
