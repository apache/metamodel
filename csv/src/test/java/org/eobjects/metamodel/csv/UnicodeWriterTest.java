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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Writer;

import org.eobjects.metamodel.util.UnicodeWriter;
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
