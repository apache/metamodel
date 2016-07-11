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

import java.io.IOException;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.List;

public class FixedWidthLineParser {
 
    private final int _fixedValueWidth;
    private final int[] _valueWidths;
    private final boolean _failOnInconsistentLineWidth;
    private final int _expectedLineLength;
    private final boolean _constantWidth;
    private volatile int _rowNumber;
    
    
    public FixedWidthLineParser(int fixedValueWidth, int[] valueWidths, boolean failOnInconsistentLineWidth, int expectedLineLength, int rowNumber, boolean constantWidth) {
        _fixedValueWidth = fixedValueWidth; 
        _valueWidths = valueWidths; 
        _failOnInconsistentLineWidth = failOnInconsistentLineWidth; 
        _expectedLineLength = expectedLineLength; 
        _rowNumber = rowNumber;         _constantWidth = constantWidth; 
    }
    
    
    public String[] parseLine(String line) throws IOException {


        final List<String> values = new ArrayList<String>();
    
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
            if (_constantWidth) {
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

        if (!_failOnInconsistentLineWidth && !_constantWidth) {
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
            if (_constantWidth) {
                if (line.length() % _fixedValueWidth != 0) {
                    throw new InconsistentValueWidthException(result, line,
                            _rowNumber);
                }
            } else {
                if (result.length != values.size()) {
                    throw new InconsistentValueWidthException(result, line,
                            _rowNumber);
                }

                if (line.length() != _expectedLineLength) {
                    throw new InconsistentValueWidthException(result, line,
                            _rowNumber);
                }
            }
        }

        return result;
}
}
