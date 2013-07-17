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
package org.eobjects.metamodel.csv;

import org.eobjects.metamodel.util.Resource;

/**
 * This class is an adaptation of the CSVWriter class of OpenCSV. We've made the
 * writer work without having the output stream as state (suiting for
 * {@link Resource} usage).
 */
public final class CsvWriter {

    public static final int INITIAL_STRING_SIZE = 128;

    private final CsvConfiguration _configuration;

    public CsvWriter(CsvConfiguration configuration) {
        _configuration = configuration;
    }

    /**
     * Builds a line for the CSV file output
     * 
     * @param nextLine
     *            a string array with each comma-separated element as a separate
     *            entry.
     */
    public String buildLine(String[] nextLine) {
        final StringBuilder sb = new StringBuilder(INITIAL_STRING_SIZE);
        for (int i = 0; i < nextLine.length; i++) {

            if (i != 0) {
                sb.append(_configuration.getSeparatorChar());
            }

            final String nextElement = nextLine[i];
            if (nextElement == null) {
                continue;
            }
            final char quoteChar = _configuration.getQuoteChar();
            if (quoteChar != CsvConfiguration.NOT_A_CHAR) {
                sb.append(quoteChar);
            }

            sb.append(stringContainsSpecialCharacters(nextElement) ? processLine(nextElement) : nextElement);

            if (quoteChar != CsvConfiguration.NOT_A_CHAR) {
                sb.append(quoteChar);
            }
        }

        sb.append('\n');
        return sb.toString();

    }

    private boolean stringContainsSpecialCharacters(String line) {
        return line.indexOf(_configuration.getQuoteChar()) != -1 || line.indexOf(_configuration.getEscapeChar()) != -1;
    }

    private StringBuilder processLine(String nextElement) {
        final StringBuilder sb = new StringBuilder(INITIAL_STRING_SIZE);
        for (int j = 0; j < nextElement.length(); j++) {
            final char nextChar = nextElement.charAt(j);
            final char escapeChar = _configuration.getEscapeChar();
            if (escapeChar != CsvConfiguration.NOT_A_CHAR && nextChar == _configuration.getQuoteChar()) {
                sb.append(escapeChar).append(nextChar);
            } else if (escapeChar != CsvConfiguration.NOT_A_CHAR && nextChar == escapeChar) {
                sb.append(escapeChar).append(nextChar);
            } else {
                sb.append(nextChar);
            }
        }

        return sb;
    }
}
