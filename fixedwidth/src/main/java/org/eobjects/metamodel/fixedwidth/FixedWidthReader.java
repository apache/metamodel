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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.List;

/**
 * Reader capable of separating values based on a fixed width setting.
 * 
 * @author Kasper Sørensen
 */
final class FixedWidthReader implements Closeable {

	private final BufferedReader _reader;
	private final int _fixedValueWidth;
	private final int[] _valueWidths;
	private final boolean _failOnInconsistentLineWidth;
	private final int expectedLineLength;
	private final boolean constantWidth;
	private volatile int _rowNumber;

	public FixedWidthReader(Reader reader, int fixedValueWidth,
			boolean failOnInconsistentLineWidth) {
		this(new BufferedReader(reader), fixedValueWidth,
				failOnInconsistentLineWidth);
	}

	public FixedWidthReader(BufferedReader reader, int fixedValueWidth,
			boolean failOnInconsistentLineWidth) {
		_reader = reader;
		_fixedValueWidth = fixedValueWidth;
		_failOnInconsistentLineWidth = failOnInconsistentLineWidth;
		_rowNumber = 0;
		_valueWidths = null;

		constantWidth = true;
		expectedLineLength = -1;
	}

	public FixedWidthReader(Reader reader, int[] valueWidths,
			boolean failOnInconsistentLineWidth) {
		this(new BufferedReader(reader), valueWidths,
				failOnInconsistentLineWidth);
	}

	public FixedWidthReader(BufferedReader reader, int[] valueWidths,
			boolean failOnInconsistentLineWidth) {
		_reader = reader;
		_fixedValueWidth = -1;
		_valueWidths = valueWidths;
		_failOnInconsistentLineWidth = failOnInconsistentLineWidth;
		_rowNumber = 0;

		constantWidth = false;
		int expectedLineLength = 0;
		if (_fixedValueWidth == -1) {
			for (int i = 0; i < _valueWidths.length; i++) {
				expectedLineLength += _valueWidths[i];
			}
		}
		this.expectedLineLength = expectedLineLength;
	}

	/***
	 * Reads the next line in the file.
	 * 
	 * @return an array of values in the next line, or null if the end of the
	 *         file has been reached.
	 * 
	 * @throws IllegalStateException
	 *             if an exception occurs while reading the file.
	 */
	public String[] readLine() throws IllegalStateException {

		try {
			final List<String> values = new ArrayList<String>();
			final String line = _reader.readLine();
			if (line == null) {
				return null;
			}

			StringBuilder nextValue = new StringBuilder();

			int valueIndex = 0;

			final CharacterIterator it = new StringCharacterIterator(line);
			for (char c = it.first(); c != CharacterIterator.DONE; c = it
					.next()) {
				nextValue.append(c);

				final int valueWidth;
				if (constantWidth) {
					valueWidth = _fixedValueWidth;
				} else {
					if (valueIndex >= _valueWidths.length) {
						if (_failOnInconsistentLineWidth) {
							String[] result = values.toArray(new String[values
									.size()]);
							throw new InconsistentValueWidthException(result,
									line, _rowNumber + 1);
						} else {
							// silently ignore the inconsistency
							break;
						}
					}
					valueWidth = _valueWidths[valueIndex];
				}

				if (nextValue.length() == valueWidth) {
					// write the value
					values.add(nextValue.toString().trim());
					nextValue = new StringBuilder();
					valueIndex++;
				}
			}

			if (nextValue.length() > 0) {
				values.add(nextValue.toString().trim());
			}

			String[] result = values.toArray(new String[values.size()]);

			if (!_failOnInconsistentLineWidth && !constantWidth) {
				if (result.length != _valueWidths.length) {
					String[] correctedResult = new String[_valueWidths.length];
					for (int i = 0; i < result.length
							&& i < _valueWidths.length; i++) {
						correctedResult[i] = result[i];
					}
					result = correctedResult;
				}
			}

			if (_failOnInconsistentLineWidth) {
				_rowNumber++;
				if (constantWidth) {
					if (line.length() % _fixedValueWidth != 0) {
						throw new InconsistentValueWidthException(result, line,
								_rowNumber);
					}
				} else {
					if (result.length != values.size()) {
						throw new InconsistentValueWidthException(result, line,
								_rowNumber);
					}

					if (line.length() != expectedLineLength) {
						throw new InconsistentValueWidthException(result, line,
								_rowNumber);
					}
				}
			}

			return result;
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void close() throws IOException {
		_reader.close();
	}

}
