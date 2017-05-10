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
import java.io.IOException;

/**
 * Reader capable of separating values based on a fixed width setting.
 */
class EbcdicReader extends FixedWidthReader {

    private final BufferedInputStream _stream;
    private final String _charsetName;
    private final boolean _skipEbcdicHeader;
    private final boolean _eolPresent;
    private boolean _headerSkipped;
    
    public EbcdicReader(BufferedInputStream stream, String charsetName, int[] valueWidths,
            boolean failOnInconsistentLineWidth, boolean skipEbcdicHeader, boolean eolPresent) {
        super(stream, charsetName, valueWidths, failOnInconsistentLineWidth);
        _stream = stream;
        _charsetName = charsetName;
        _skipEbcdicHeader = skipEbcdicHeader;
        _eolPresent = eolPresent;
    }

    @Override
    protected void beforeReadLine() {
        if (shouldSkipHeader()) {
            try {
                skipHeader();
            } catch (IOException e) {
                throw new IllegalStateException("A problem occurred while skipping the input stream. ", e); 
            }
        }
    }

    private boolean shouldSkipHeader() {
        return (_skipEbcdicHeader && !_headerSkipped);
    }

    private void skipHeader() throws IOException {
        _headerSkipped = true;
        _stream.skip(_expectedLineLength);
    }

    @Override
    protected String readSingleRecordData() throws IOException {
        if (_eolPresent) {
            return super.readSingleRecordData();
        } else {
            byte[] buffer = new byte[_expectedLineLength];
            int bytesRead = _stream.read(buffer, 0, _expectedLineLength);

            if (bytesRead < 0) {
                return null;
            }

            return new String(buffer, _charsetName);
        } 
    }
}
