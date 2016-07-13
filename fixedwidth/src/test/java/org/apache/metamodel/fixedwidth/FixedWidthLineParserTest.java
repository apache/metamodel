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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FixedWidthLineParserTest {
    
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testParser() throws IOException {
        int[] widths = new int[] { 8, 9 };
        FixedWidthConfiguration fixedWidthConfiguration = new FixedWidthConfiguration(FixedWidthConfiguration.NO_COLUMN_NAME_LINE, null, widths, false); 
        final FixedWidthLineParser parser = new FixedWidthLineParser(fixedWidthConfiguration, 17, 0); 

        final String lineToParse1 = "greeting  greeter  ";
        final String[] line = parser.parseLine(lineToParse1);
        assertEquals("[greeting, greeter]", Arrays.asList(line).toString());
        
        final String lineToParse2="howdy     partner"; 
        String[] line2 = parser.parseLine(lineToParse2);
        assertEquals("[howdy, partner]", Arrays.asList(line2).toString()); 
        
        final String lineToParse3 ="hi        there "; 
        String[] line3 = parser.parseLine(lineToParse3);
        assertEquals("[hi, there]", Arrays.asList(line3).toString()); 
        
    }
    
    @Test
    public void testParserFailInconsistentRowException() throws IOException {
        int[] widths = new int[] { 8, 5 };
        FixedWidthConfiguration fixedWidthConfiguration = new FixedWidthConfiguration(FixedWidthConfiguration.NO_COLUMN_NAME_LINE, null, widths, true); 
        final FixedWidthLineParser parser = new FixedWidthLineParser(fixedWidthConfiguration, 17, 0); 

        final String lineToParse1 = "greeting  greeter  ";
        exception.expect(InconsistentValueWidthException.class);
        @SuppressWarnings("unused")
        final String[] line = parser.parseLine(lineToParse1);
    }
}
