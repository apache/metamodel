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
	private final FixedWidthLineParser _parser; 

	public FixedWidthReader(Reader reader, int fixedValueWidth,
			boolean failOnInconsistentLineWidth) {
		this(new BufferedReader(reader), fixedValueWidth,
				failOnInconsistentLineWidth);
	}

	public FixedWidthReader(BufferedReader reader, int fixedValueWidth,
			boolean failOnInconsistentLineWidth) {
		_reader = reader;
        final FixedWidthConfiguration fixedWidthConfiguration = new FixedWidthConfiguration(
                FixedWidthConfiguration.NO_COLUMN_NAME_LINE, null, fixedValueWidth, failOnInconsistentLineWidth);
        _parser = new FixedWidthLineParser(fixedWidthConfiguration, -1, 0);
	}

	public FixedWidthReader(Reader reader, int[] valueWidths,
			boolean failOnInconsistentLineWidth) {
		this(new BufferedReader(reader), valueWidths,
				failOnInconsistentLineWidth);
	}

	public FixedWidthReader(BufferedReader reader, int[] valueWidths,
			boolean failOnInconsistentLineWidth) {
		_reader = reader;
		int fixedValueWidth = -1;
		int expectedLineLength = 0;
		if (fixedValueWidth == -1) {
			for (int i = 0; i < valueWidths.length; i++) {
				expectedLineLength += valueWidths[i];
			}
		}
        final FixedWidthConfiguration fixedWidthConfiguration = new FixedWidthConfiguration(
                FixedWidthConfiguration.NO_COLUMN_NAME_LINE, null, valueWidths, failOnInconsistentLineWidth);
        _parser = new FixedWidthLineParser(fixedWidthConfiguration, expectedLineLength, 0);
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
