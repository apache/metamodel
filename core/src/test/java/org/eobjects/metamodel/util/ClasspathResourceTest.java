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

import java.io.InputStream;

import junit.framework.TestCase;

public class ClasspathResourceTest extends TestCase {

    public void testGetName() throws Exception {
        ClasspathResource resource = new ClasspathResource("folder/foo");
        assertEquals("foo", resource.getName());
        assertTrue(resource.isExists());
        assertTrue(resource.isReadOnly());
        
        resource = new ClasspathResource("/folder/foo");
        assertEquals("foo", resource.getName());
        assertTrue(resource.isExists());
        assertTrue(resource.isReadOnly());
        
        String result = resource.read(new Func<InputStream, String>() {
            @Override
            public String eval(InputStream inputStream) {
                return FileHelper.readInputStreamAsString(inputStream, "UTF8");
            }
        });
        assertEquals("bar-baz", result);
    }
}
