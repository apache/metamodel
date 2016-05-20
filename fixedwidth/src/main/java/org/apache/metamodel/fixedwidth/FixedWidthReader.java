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
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.List;

/**
 * Reader capable of separating values based on a fixed width setting.
 */
final class FixedWidthReader implements Closeable {
    private static final int END_OF_STREAM = -1;
    private static final int LINE_FEED = '\n';
    private static final int CARRIAGE_RETURN = '\r';
    private final BufferedInputStream _stream;
    private final String _charsetName;
    private final int _fixedValueWidth;
    private final int[] _valueWidths;
    private int _valueIndex = 0;
    private final boolean _headerPresent;
    private final boolean _eolPresent;
    private final boolean _failOnInconsistentLineWidth;
    private final int _expectedLineLength;
    private final boolean _constantWidth;
    private volatile int _rowNumber;
    private boolean _headerSkipped;

    public FixedWidthReader(InputStream stream, String charsetName, int fixedValueWidth,
            boolean failOnInconsistentLineWidth, boolean headerPresent, boolean eolPresent) {
        this(new BufferedInputStream(stream), charsetName, fixedValueWidth, failOnInconsistentLineWidth, headerPresent,
                eolPresent);
    }

    public FixedWidthReader(BufferedInputStream stream, String charsetName, int fixedValueWidth,
            boolean failOnInconsistentLineWidth, boolean headerPresent, boolean eolPresent) {
        _stream = stream;
        _charsetName = charsetName;
        _fixedValueWidth = fixedValueWidth;
        _failOnInconsistentLineWidth = failOnInconsistentLineWidth;
        _headerPresent = headerPresent;
        _eolPresent = eolPresent;
        _rowNumber = 0;
        _valueWidths = null;
        _constantWidth = true;
        _expectedLineLength = -1;
    }

    public FixedWidthReader(InputStream stream, String charsetName, int[] valueWidths,
            boolean failOnInconsistentLineWidth, boolean headerPresent, boolean eolPresent) {
        this(new BufferedInputStream(stream), charsetName, valueWidths, failOnInconsistentLineWidth, headerPresent,
                eolPresent);
    }

    public FixedWidthReader(BufferedInputStream stream, String charsetName, int[] valueWidths,
            boolean failOnInconsistentLineWidth, boolean headerPresent, boolean eolPresent) {
        _stream = stream;
        _charsetName = charsetName;
        _fixedValueWidth = -1;
        _valueWidths = valueWidths;
        _failOnInconsistentLineWidth = failOnInconsistentLineWidth;
        _headerPresent = headerPresent;
        _eolPresent = eolPresent;
        _rowNumber = 0;
        _constantWidth = false;
        int expectedLineLength = 0;

        for (int i = 0; i < _valueWidths.length; i++) {
            expectedLineLength += _valueWidths[i];
        }

        _expectedLineLength = expectedLineLength;
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
            if (shouldSkipHeader()) {
                skipHeader();
            }

            return getValues();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private boolean shouldSkipHeader() {
        return (_headerPresent && !_headerSkipped);
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

        if (_failOnInconsistentLineWidth) {
            throwInconsistentValueException(singleRecordData, result, values.size());
        }

        return result;
    }

    private void throwInconsistentValueException(String singleRecordData, String[] result, int valuesSize) {
        InconsistentValueWidthException inconsistentValueException =
                buildInconsistentValueWidthException(singleRecordData, result, valuesSize);

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

    private String readSingleRecordData() throws IOException {
        if (isEOLAvailable()) {
            StringBuilder line = new StringBuilder();
            int ch;

            for (ch = _stream.read(); !isEndingCharacter(ch); ch = _stream.read()) {
                line.append((char) ch);
            }

            if (ch == CARRIAGE_RETURN) {
                readLineFeedIfFollows();
            }

            return (line.length()) > 0 ? line.toString() : null;
        } else {
            byte[] buffer = new byte[_expectedLineLength];
            int bytesRead = _stream.read(buffer, 0, _expectedLineLength);

            if (bytesRead < 0) {
                return null;
            }

            return new String(buffer, _charsetName);
        }
    }

    private void readLineFeedIfFollows() throws IOException {
        _stream.mark(1);

        if (_stream.read() != LINE_FEED) {
            _stream.reset();
        }
    }

    private boolean isEndingCharacter(int ch) {
        return (ch == CARRIAGE_RETURN || ch == LINE_FEED || ch == END_OF_STREAM);
    }

    private boolean isEOLAvailable() {
        return _eolPresent;
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

    private InconsistentValueWidthException buildInconsistentValueWidthException(String recordData, String[] result,
            int valuesSize) {
        _rowNumber++;

        if (_constantWidth) {
            if (recordData.length() % _fixedValueWidth != 0) {
                return new InconsistentValueWidthException(result, recordData, _rowNumber);
            }
        } else {
            if (result.length != valuesSize) {
                return new InconsistentValueWidthException(result, recordData, _rowNumber);
            }

            if (recordData.length() != _expectedLineLength) {
                return new InconsistentValueWidthException(result, recordData, _rowNumber);
            }
        }

        return null;
    }

    private void skipHeader() throws IOException {
        _headerSkipped = true;
        _stream.skip(_expectedLineLength);
    }

    @Override
    public void close() throws IOException {
        _stream.close();
    }
}
