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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.StringTokenizer;

import junit.framework.TestCase;

public class SerializableRefTest extends TestCase {

    public void testSerialize() throws Exception {
        SerializableRef<String> ref = new SerializableRef<String>("Foobar");
        assertNotNull(ref.get());

        SerializableRef<String> copy = copy(ref);
        assertEquals("Foobar", copy.get());
    }

    public void testDontSerialize() throws Exception {
        SerializableRef<StringTokenizer> ref = new SerializableRef<StringTokenizer>(new StringTokenizer("foobar"));
        assertNotNull(ref.get());

        SerializableRef<StringTokenizer> copy = copy(ref);
        assertNull(copy.get());
    }

    @SuppressWarnings("unchecked")
    private <E> SerializableRef<E> copy(SerializableRef<E> ref) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(baos);
        os.writeObject(ref);
        os.flush();
        os.close();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream is = new ObjectInputStream(bais);
        Object obj = is.readObject();
        is.close();
        return (SerializableRef<E>) obj;
    }
}
