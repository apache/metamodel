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
package org.apache.metamodel.fixedwidth;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;

/**
 * Reader capable of separating values based on a fixed width setting.
 */
final public class FixedWidthReader implements Closeable {

	private final BufferedReader _reader;
	private final int _fixedValueWidth;
	private final int[] _valueWidths;
	private final boolean _failOnInconsistentLineWidth;
	private final int _expectedLineLength;
	private final boolean _constantWidth;
	private volatile int _rowNumber;
	private final transient FixedWidthLineParser _parser; 

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

		_constantWidth = true;
		_expectedLineLength = -1;
        _parser = new FixedWidthLineParser(_fixedValueWidth, _valueWidths, _failOnInconsistentLineWidth, _expectedLineLength, _rowNumber, _constantWidth);
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

		_constantWidth = false;
		int expectedLineLength = 0;
		if (_fixedValueWidth == -1) {
			for (int i = 0; i < _valueWidths.length; i++) {
				expectedLineLength += _valueWidths[i];
			}
		}
		_expectedLineLength = expectedLineLength;
		_parser = new FixedWidthLineParser(_fixedValueWidth, _valueWidths, _failOnInconsistentLineWidth, _expectedLineLength, _rowNumber, _constantWidth);
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
        String line;
        try {
            line = _reader.readLine();
            return _parser.parseLine(line);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
	}
	

	@Override
	public void close() throws IOException {
		_reader.close();
	}

}
