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

import java.io.InputStream;
import java.io.OutputStream;

import junit.framework.TestCase;

public class InMemoryResourceTest extends TestCase {

    public void testScenario() throws Exception {
        InMemoryResource r = new InMemoryResource("foo/bar");
        assertEquals("bar", r.getName());
        assertEquals(-1, r.getLastModified());
        assertEquals(0, r.getSize());
        assertFalse(r.isReadOnly());
        assertTrue(r.isExists());

        r.write(new Action<OutputStream>() {
            @Override
            public void run(OutputStream out) throws Exception {
                out.write(1);
                out.write(2);
                out.write(3);
            }
        });

        assertEquals(3, r.getSize());

        r.read(new Action<InputStream>() {
            @Override
            public void run(InputStream in) throws Exception {
                assertEquals(1, in.read());
                assertEquals(2, in.read());
                assertEquals(3, in.read());
                assertEquals(-1, in.read());
            }
        });

        r.append(new Action<OutputStream>() {
            @Override
            public void run(OutputStream out) throws Exception {
                out.write(4);
                out.write(5);
                out.write(6);
            }
        });

        r.read(new Action<InputStream>() {
            @Override
            public void run(InputStream in) throws Exception {
                assertEquals(1, in.read());
                assertEquals(2, in.read());
                assertEquals(3, in.read());
                assertEquals(4, in.read());
                assertEquals(5, in.read());
                assertEquals(6, in.read());
                assertEquals(-1, in.read());
            }
        });
    }
}
