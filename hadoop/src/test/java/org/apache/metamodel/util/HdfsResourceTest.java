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

import junit.framework.TestCase;

public class HdfsResourceTest extends TestCase {

    public void testGetQualifiedName() throws Exception {
        final HdfsResource res1 = new HdfsResource("hdfs://localhost:9000/home/metamodel.txt");
        assertEquals("hdfs://localhost:9000/home/metamodel.txt", res1.getQualifiedPath());
        assertEquals("metamodel.txt", res1.getName());

        final HdfsResource res2 = new HdfsResource("localhost", 9000, "/home/metamodel.txt");
        assertEquals("hdfs://localhost:9000/home/metamodel.txt", res2.getQualifiedPath());
        assertEquals("metamodel.txt", res2.getName());

        assertEquals(res1, res2);

        final HdfsResource res3 = new HdfsResource("localhost", 9000, "/home/apache.txt");
        assertEquals("hdfs://localhost:9000/home/apache.txt", res3.getQualifiedPath());
        assertEquals("apache.txt", res3.getName());

        assertFalse(res3.equals(res1));
    }
}
