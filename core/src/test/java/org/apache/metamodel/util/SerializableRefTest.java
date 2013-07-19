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
