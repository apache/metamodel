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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.List;

/**
 * Reader capable of separating values based on a fixed width setting.
 */
class FixedWidthReader implements Closeable {
    private static final int END_OF_STREAM = -1;
    private static final int LINE_FEED = '\n';
    private static final int CARRIAGE_RETURN = '\r';
    
    private final int _fixedValueWidth;
    private final int[] _valueWidths;
    private int _valueIndex = 0;
    private final boolean _failOnInconsistentLineWidth;
    private final boolean _constantWidth;
    private volatile int _rowNumber;
    protected final Reader _reader;
    protected final int _expectedLineLength;

    public FixedWidthReader(InputStream stream, String charsetName, int fixedValueWidth,
            boolean failOnInconsistentLineWidth) {
        this(new BufferedInputStream(stream), charsetName, fixedValueWidth, failOnInconsistentLineWidth);
    }

    private FixedWidthReader(BufferedInputStream stream, String charsetName, int fixedValueWidth,
            boolean failOnInconsistentLineWidth) {
        _reader = initReader(stream, charsetName);
        _fixedValueWidth = fixedValueWidth;
        _failOnInconsistentLineWidth = failOnInconsistentLineWidth;
        _rowNumber = 0;
        _valueWidths = null;
        _constantWidth = true;
        _expectedLineLength = -1;
    }

    public FixedWidthReader(InputStream stream, String charsetName, int[] valueWidths,
            boolean failOnInconsistentLineWidth) {
        this(new BufferedInputStream(stream), charsetName, valueWidths, failOnInconsistentLineWidth);
    }

    FixedWidthReader(BufferedInputStream stream, String charsetName, int[] valueWidths,
            boolean failOnInconsistentLineWidth) {
        _reader = initReader(stream, charsetName);
        _fixedValueWidth = -1;
        _valueWidths = valueWidths;
        _failOnInconsistentLineWidth = failOnInconsistentLineWidth;
        _rowNumber = 0;
        _constantWidth = false;
        int expectedLineLength = 0;

        for (final int _valueWidth : _valueWidths) {
            expectedLineLength += _valueWidth;
        }

        _expectedLineLength = expectedLineLength;
    }

    private Reader initReader(BufferedInputStream stream, String charsetName) {
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(stream, charsetName);
            return new BufferedReader(inputStreamReader);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(String.format("Encoding '%s' was not recognized. ", charsetName));
        }
    }
    
    /**
     * This reads and returns the next record from the file. Usually, it is a line but in case the new line characters
     * are not present, the length of the content depends on the column-widths setting.
     *
     * @return an array of values in the next line, or null if the end of the file has been reached.
     * @throws IllegalStateException if an exception occurs while reading the file.
     */
    public String[] readLine() throws IllegalStateException {
        try {
            beforeReadLine();
            _rowNumber++;
            return getValues();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Empty hook that enables special behavior in sub-classed readers (by overriding this method). 
     */
    protected void beforeReadLine() {
    }

    private String[] getValues() throws IOException {
        final List<String> values = new ArrayList<>();
        final String singleRecordData = readSingleRecordData();

        if (singleRecordData == null) {
            return null;
        }

        processSingleRecordData(singleRecordData, values);
        String[] result = values.toArray(new String[values.size()]);

        if (!_failOnInconsistentLineWidth && !_constantWidth) {
            result = correctResult(result);
        }

        validateConsistentValue(singleRecordData, result, values.size());

        return result;
    }

    private void validateConsistentValue(String recordData, String[] result, int valuesSize) {
        if (!_failOnInconsistentLineWidth) {
            return;
        }

        InconsistentValueWidthException inconsistentValueException = null;

        if (_constantWidth) {
            if (recordData.length() % _fixedValueWidth != 0) {
                inconsistentValueException = new InconsistentValueWidthException(result, recordData, _rowNumber);
            }
        } else if (result.length != valuesSize || recordData.length() != _expectedLineLength) {
            inconsistentValueException = new InconsistentValueWidthException(result, recordData, _rowNumber);
        }

        if (inconsistentValueException != null) {
            throw inconsistentValueException;
        }
    }

    private void processSingleRecordData(final String singleRecordData, final List<String> values) {
        StringBuilder nextValue = new StringBuilder();
        final CharacterIterator it = new StringCharacterIterator(singleRecordData);
        _valueIndex = 0;

        for (char c = it.first(); c != CharacterIterator.DONE; c = it.next()) {
            processCharacter(c, nextValue, values, singleRecordData);
        }

        if (nextValue.length() > 0) {
            addNewValueIfAppropriate(values, nextValue);
        }
    }

    String readSingleRecordData() throws IOException {
        StringBuilder line = new StringBuilder();
        int ch;

        for (ch = _reader.read(); !isEndingCharacter(ch); ch = _reader.read()) {
            line.append((char)ch);
        }

        if (ch == CARRIAGE_RETURN) {
            readLineFeedIfFollows();
        }

        return (line.length()) > 0 ? line.toString() : null;
    }
    
    private void readLineFeedIfFollows() throws IOException {
        _reader.mark(1);
        
        if (_reader.read() != LINE_FEED) {
            _reader.reset();
        }
    }

    private boolean isEndingCharacter(int ch) {
        return (ch == CARRIAGE_RETURN || ch == LINE_FEED || ch == END_OF_STREAM);
    }
    
    private void processCharacter(char c, StringBuilder nextValue, List<String> values, String recordData) {
        nextValue.append(c);
        final int valueWidth = getValueWidth(values, recordData);

        if (nextValue.length() == valueWidth) {
            addNewValueIfAppropriate(values, nextValue);
            nextValue.setLength(0); // clear the buffer

            if (_valueWidths != null) {
                _valueIndex = (_valueIndex + 1) % _valueWidths.length;
            }
        }
    }

    private int getValueWidth(List<String> values, String recordData) {
        if (_constantWidth) {
            return _fixedValueWidth;
        } else {
            if (_valueIndex >= _valueWidths.length) {
                if (_failOnInconsistentLineWidth) {
                    String[] result = values.toArray(new String[values.size()]);
                    throw new InconsistentValueWidthException(result, recordData, _rowNumber + 1);
                } else {
                    return -1; // silently ignore the inconsistency
                }
            }

            return _valueWidths[_valueIndex];
        }
    }

    private void addNewValueIfAppropriate(List<String> values, StringBuilder nextValue) {
        if (_valueWidths != null) {
            if (values.size() < _valueWidths.length) {
                values.add(nextValue.toString().trim());
            }
        } else {
            values.add(nextValue.toString().trim());
        }
    }

    private String[] correctResult(String[] result) {
        if (result.length != _valueWidths.length) {
            String[] correctedResult = new String[_valueWidths.length];

            for (int i = 0; i < result.length && i < _valueWidths.length; i++) {
                correctedResult[i] = result[i];
            }

            result = correctedResult;
        }

        return result;
    }

    @Override
    public void close() throws IOException {
        _reader.close();
    }
}
