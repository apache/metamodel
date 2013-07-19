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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Writer;

import org.apache.metamodel.util.UnicodeWriter;
import org.junit.Test;

public class UnicodeWriterTest {

    @Test
    public void test() throws IOException {
        File file = new File("target/unicodeWriterTest.txt");
        Writer writer = new UnicodeWriter(file, "UTF-8");
        writer.write("Hello");
        writer.close();

        FileInputStream is = new FileInputStream(file);
        byte[] bytes = new byte[100];
        assertEquals(8, is.read(bytes));

        assertEquals(UnicodeWriter.UTF8_BOM[0], bytes[0]);
        assertEquals(UnicodeWriter.UTF8_BOM[1], bytes[1]);
        assertEquals(UnicodeWriter.UTF8_BOM[2], bytes[2]);
        assertEquals((byte) 'H', bytes[3]);
        
        is.close();
    }
}
