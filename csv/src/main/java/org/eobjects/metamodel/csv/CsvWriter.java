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
