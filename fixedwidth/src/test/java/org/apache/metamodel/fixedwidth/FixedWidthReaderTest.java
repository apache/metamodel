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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FixedWidthReaderTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    @Test
    public void testBufferedReader1() throws IOException {
        final File file = new File("src/test/resources/example_simple1.txt");
        final BufferedReader reader = new BufferedReader(new FileReader(file));
        int[] widths = new int[] { 8, 9 };
        try (final FixedWidthReader fixedWidthReader = new FixedWidthReader(reader, widths, false)) {
            final String[] line1 = fixedWidthReader.readLine();
            assertEquals("[greeting, greeter]", Arrays.asList(line1).toString());
            final String[] line2 = fixedWidthReader.readLine();
            assertEquals("[hello, world]", Arrays.asList(line2).toString());
            final String[] line3 = fixedWidthReader.readLine();
            assertEquals("[hi, there]", Arrays.asList(line3).toString());
        }
    }
    
    @Test
    public void testBufferedReader2() throws IOException {
        final File file = new File("src/test/resources/example_simple2.txt");
        final BufferedReader reader = new BufferedReader(new FileReader(file));
        int[] widths = new int[] {1, 8, 9 };
        try (final FixedWidthReader fixedWidthReader = new FixedWidthReader(reader, widths, false)) {
            final String[] line1 = fixedWidthReader.readLine();
            assertEquals("[i, greeting, greeter]", Arrays.asList(line1).toString());
            final String[] line2 = fixedWidthReader.readLine();
            assertEquals("[1, hello, world]", Arrays.asList(line2).toString());
            final String[] line3 = fixedWidthReader.readLine();
            assertEquals("[2, hi, there]", Arrays.asList(line3).toString());
        }
    }
    
    @Test
    public void testBufferedReader3() throws IOException {
        final File file = new File("src/test/resources/example_simple3.txt");
        final BufferedReader reader = new BufferedReader(new FileReader(file));
        try (final FixedWidthReader fixedWidthReader = new FixedWidthReader(reader, 5, false)) {
            final String[] line1 = fixedWidthReader.readLine();
            assertEquals("[hello]", Arrays.asList(line1).toString());
            final String[] line2 = fixedWidthReader.readLine();
            assertEquals("[world]", Arrays.asList(line2).toString());
            final String[] line3 = fixedWidthReader.readLine();
            assertEquals("[howdy]", Arrays.asList(line3).toString());
            final String[] line4 = fixedWidthReader.readLine();
            assertEquals("[ther]", Arrays.asList(line4).toString());
        }
    }
    
    @Test
    public void testBufferedReaderFailOnInconsistentRows() throws IOException {
        final File file = new File("src/test/resources/example_simple3.txt");
        final BufferedReader reader = new BufferedReader(new FileReader(file));
        try (final FixedWidthReader fixedWidthReader = new FixedWidthReader(reader, 5, true)) {
            final String[] line1 = fixedWidthReader.readLine();
            assertEquals("[hello]", Arrays.asList(line1).toString());
            final String[] line2 = fixedWidthReader.readLine();
            assertEquals("[world]", Arrays.asList(line2).toString());
            final String[] line3 = fixedWidthReader.readLine();
            assertEquals("[howdy]", Arrays.asList(line3).toString());
           
            exception.expect(InconsistentValueWidthException.class);            
            @SuppressWarnings("unused")
            final String[] line4 = fixedWidthReader.readLine();
        }
    }

   
}
