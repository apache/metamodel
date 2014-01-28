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

public class ResourceUtilsTest extends TestCase {

    public void testGetParentName() throws Exception {
        Resource res = new FileResource(new File("src/test/resources/folder/foo"));

        assertEquals("folder", ResourceUtils.getParentName(res));

        res = new FileResource(new File("src/test/resources/folder/"));

        assertEquals("resources", ResourceUtils.getParentName(res));

        assertEquals("", ResourceUtils.getParentName(new InMemoryResource("foo")));
        assertEquals("bar", ResourceUtils.getParentName(new InMemoryResource("foo/bar\\baz")));
    }

    public void testGetParentNameRootFile() throws Exception {
        assertEquals("c:", ResourceUtils.getParentName(new InMemoryResource("c:\\foo.txt")));
        assertEquals("/", ResourceUtils.getParentName(new InMemoryResource("/foo.txt")));
    }

    public void testGetParentNameOddPaths() throws Exception {
        assertEquals("", ResourceUtils.getParentName(new InMemoryResource("////")));
        assertEquals("", ResourceUtils.getParentName(new InMemoryResource("")));
        assertEquals("", ResourceUtils.getParentName(new InMemoryResource("/")));
        assertEquals("", ResourceUtils.getParentName(new InMemoryResource("//")));
    }
}
