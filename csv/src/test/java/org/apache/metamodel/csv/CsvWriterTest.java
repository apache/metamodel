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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class CsvWriterTest {

    @Test
    public void testBuildLineWithSeparatorInValue() throws Exception {
        CsvConfiguration configuration = new CsvConfiguration(1, "UTF-8", ',', '"', '\\');
        CsvWriter writer = new CsvWriter(configuration);
        String line = writer.buildLine(new String[] {"0", "1,2", "3'4", "5\\6"});
        assertEquals("\"0\",\"1,2\",\"3'4\",\"5\\\\6\"\n", line);
    }
    

    @Test
    public void testBuildLineWithSeparatorInValueAndNoQuoteCharacterSet() throws Exception {
        CsvConfiguration configuration = new CsvConfiguration(1, "UTF-8", ',', CsvConfiguration.NOT_A_CHAR, '\\');
        CsvWriter writer = new CsvWriter(configuration);
        String line = writer.buildLine(new String[] {"0", "1,2", "3'4", "5\\6"});
        assertEquals("0,1\\,2,3'4,5\\\\6\n", line);
    }
}
