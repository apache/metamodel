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
package org.eobjects.metamodel.util;

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