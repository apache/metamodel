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

import org.apache.metamodel.schema.naming.ColumnNamingStrategy;

/**
 * Special fixed-width configuration for EBCDIC files. 
 */
public final class EbcdicConfiguration extends FixedWidthConfiguration {

    private static final long serialVersionUID = 1L;
    
    private final boolean _skipEbcdicHeader;
    private final boolean _eolPresent;

    public EbcdicConfiguration(int columnNameLineNumber, String encoding, int fixedValueWidth,
            boolean failOnInconsistentLineWidth, boolean skipEbcdicHeader, boolean eolPresent) {
        super(columnNameLineNumber, encoding, fixedValueWidth, failOnInconsistentLineWidth);
        _skipEbcdicHeader = skipEbcdicHeader;
        _eolPresent = eolPresent;
    }

    public EbcdicConfiguration(int columnNameLineNumber, String encoding, int[] valueWidths,
            boolean failOnInconsistentLineWidth, boolean skipEbcdicHeader, boolean eolPresent) {
        this(columnNameLineNumber, null, encoding, valueWidths, failOnInconsistentLineWidth, skipEbcdicHeader, eolPresent);
    }

    public EbcdicConfiguration(int columnNameLineNumber, ColumnNamingStrategy columnNamingStrategy, String encoding,
            int[] valueWidths, boolean failOnInconsistentLineWidth, boolean skipEbcdicHeader, boolean eolPresent) {
        super(columnNameLineNumber, columnNamingStrategy, encoding, valueWidths, failOnInconsistentLineWidth);
        _skipEbcdicHeader = skipEbcdicHeader;
        _eolPresent = eolPresent;
    }

    /**
     * Determines if the input file contains a header that should be skipped before reading records data.
     *
     * @return a boolean indicating whether or not to skip EBCDIC header.
     */
    public boolean isSkipEbcdicHeader() {
        return _skipEbcdicHeader;
    }

    /**
     * Determines if the input file contains new line characters.
     *
     * @return a boolean indicating whether or not the input contains new line characters.
     */
    public boolean isEolPresent() {
        return _eolPresent;
    }
}
