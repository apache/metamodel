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

import java.io.Serializable;
import java.util.List;

import org.eobjects.metamodel.util.BaseObject;
import org.eobjects.metamodel.util.FileHelper;

/**
 * Represents the configuration for reading/parsing CSV files.
 * 
 * @author Kasper Sørensen
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

	public CsvConfiguration() {
		this(DEFAULT_COLUMN_NAME_LINE);
	}

	public CsvConfiguration(int columnNameLineNumber) {
		this(columnNameLineNumber, FileHelper.DEFAULT_ENCODING,
				DEFAULT_SEPARATOR_CHAR, DEFAULT_QUOTE_CHAR, DEFAULT_ESCAPE_CHAR);
	}

	public CsvConfiguration(int columnNameLineNumber, String encoding,
			char separatorChar, char quoteChar, char escapeChar) {
		this(columnNameLineNumber, encoding, separatorChar, quoteChar,
				escapeChar, false);
	}

	public CsvConfiguration(int columnNameLineNumber, String encoding,
			char separatorChar, char quoteChar, char escapeChar,
			boolean failOnInconsistentRowLength) {
		this.columnNameLineNumber = columnNameLineNumber;
		this.encoding = encoding;
		this.separatorChar = separatorChar;
		this.quoteChar = quoteChar;
		this.escapeChar = escapeChar;
		this.failOnInconsistentRowLength = failOnInconsistentRowLength;
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
		return "CsvConfiguration[columnNameLineNumber=" + columnNameLineNumber
				+ ", encoding=" + encoding + ", separatorChar=" + separatorChar
				+ ", quoteChar=" + quoteChar + ", escapeChar=" + escapeChar
				+ ", failOnInconsistentRowLength="
				+ failOnInconsistentRowLength + "]";
	}
}
