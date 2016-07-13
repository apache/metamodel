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
 
    private final int _expectedLineLength;
    private volatile int _rowNumber;
    private final FixedWidthConfiguration _configuration; 
    
    public FixedWidthLineParser(FixedWidthConfiguration configuration, int expectedLineLength, int rowNumber) {
        _configuration = configuration; 
        _expectedLineLength = expectedLineLength;         _rowNumber = rowNumber; 
    }
    
    
    public String[] parseLine(String line) throws IOException {
        final List<String> values = new ArrayList<String>();
        int[] valueWidths = _configuration.getValueWidths();
    
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
            if (_configuration.isConstantValueWidth()) {
                valueWidth = _configuration.getFixedValueWidth();
            } else {
                if (valueIndex >= valueWidths.length) {
                    if (_configuration.isFailOnInconsistentLineWidth()) {
                        String[] result = values.toArray(new String[values
                                .size()]);
                        throw new InconsistentValueWidthException(result,
                                line, _rowNumber + 1);
                    } else {
                        // silently ignore the inconsistency
                        break;
                    }
                }
                valueWidth = _configuration.getValueWidth(valueIndex); 
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

        if (!_configuration.isFailOnInconsistentLineWidth() && ! _configuration.isConstantValueWidth()) {
            if (result.length != valueWidths.length) {
                String[] correctedResult = new String[valueWidths.length];
                for (int i = 0; i < result.length
                        && i < valueWidths.length; i++) {
                    correctedResult[i] = result[i];
                }
                result = correctedResult;
            }
        }

        if (_configuration.isFailOnInconsistentLineWidth()) {
            _rowNumber++;
            if (_configuration.isConstantValueWidth()) {
                if (line.length() % _configuration.getFixedValueWidth() != 0) {
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
