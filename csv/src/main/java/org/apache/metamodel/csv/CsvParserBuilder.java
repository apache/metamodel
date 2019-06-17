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
package org.apache.metamodel.csv;

import com.opencsv.CSVParserBuilder;
import com.opencsv.ICSVParser;
import com.opencsv.RFC4180ParserBuilder;

class CsvParserBuilder {
    private final CSVParserBuilder _csvParserBuilder;
    private final RFC4180ParserBuilder _rfc4180ParserBuilder;

    CsvParserBuilder(final CsvConfiguration csvConfiguration) {
        if (csvConfiguration.getEscapeChar() == csvConfiguration.getQuoteChar()) {
            _csvParserBuilder = null;
            _rfc4180ParserBuilder = new RFC4180ParserBuilder()
                    .withSeparator(csvConfiguration.getSeparatorChar())
                    .withQuoteChar(csvConfiguration.getQuoteChar());
        } else {
            _csvParserBuilder = new CSVParserBuilder()
                    .withSeparator(csvConfiguration.getSeparatorChar())
                    .withQuoteChar(csvConfiguration.getQuoteChar())
                    .withEscapeChar(csvConfiguration.getEscapeChar());
            _rfc4180ParserBuilder = null;
        }
    }
    
    ICSVParser build() {
        if (_csvParserBuilder == null) {
            return _rfc4180ParserBuilder.build();
        }
        return _csvParserBuilder.build(); 
    }
}
