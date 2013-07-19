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
package org.apache.metamodel.util;

import java.io.File;

import junit.framework.TestCase;

public class FileHelperTest extends TestCase {

    public void testGetTempDir() throws Exception {
        File tempDir = FileHelper.getTempDir();
        String property = System.getProperty("java.io.tmpdir");
        assertEquals(normalize(property), normalize(tempDir.getPath()));
    }

    private String normalize(String path) {
        if (path == null) {
            return null;
        }
        if (path.endsWith(File.separator)) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    public void testWriteAndRead() throws Exception {
        File file = new File("target/tmp/FileHelperTest.testWriteAndRead.txt");
        if (file.exists()) {
            file.delete();
        }
        file.getParentFile().mkdirs();
        assertTrue(file.createNewFile());
        FileHelper.writeStringAsFile(file, "foo\nbar");
        String content = FileHelper.readFileAsString(file);
        assertEquals("foo\nbar", content);
        assertTrue(file.delete());
    }

    public void testByteOrderMarksInputStream() throws Exception {
        String str1 = FileHelper.readFileAsString(new File("src/test/resources/unicode-text-utf16.txt"));
        assertEquals("hello", str1);

        String str2 = FileHelper.readFileAsString(new File("src/test/resources/unicode-text-utf8.txt"));
        assertEquals(str1, str2);

        String str3 = FileHelper.readFileAsString(new File("src/test/resources/unicode-text-utf16le.txt"));
        assertEquals(str2, str3);

        String str4 = FileHelper.readFileAsString(new File("src/test/resources/unicode-text-utf16be.txt"));
        assertEquals(str3, str4);
    }

    public void testCannotAppendAndInsertBom() throws Exception {
        try {
            FileHelper.getWriter(new File("foo"), "foo", true, true);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertEquals("Can not insert BOM into appending writer", e.getMessage());
        }
    }
}