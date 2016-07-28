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
package org.apache.metamodel.factory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.metamodel.util.ClasspathResource;
import org.apache.metamodel.util.InMemoryResource;
import org.apache.metamodel.util.Resource;
import org.apache.metamodel.util.UrlResource;
import org.junit.Test;

public class ResourceFactoryRegistryImplTest {

    private final ResourceFactoryRegistry registry = ResourceFactoryRegistryImpl.getDefaultInstance();

    @Test
    public void testGetQualifiedFileResource() throws Exception {
        final File file = new File("src/test/resources/unicode-text-utf8.txt");
        final Resource res = registry.createResource(new SimpleResourceProperties("file:///" + file.getAbsolutePath()
                .replace('\\', '/')));
        assertTrue(res.isExists());
        assertEquals("unicode-text-utf8.txt", res.getName());
    }

    @Test
    public void testGetUnqualifiedRelativeFileResource() throws Exception {
        final Resource res = registry.createResource(new SimpleResourceProperties(
                "src/test/resources/unicode-text-utf8.txt"));
        assertTrue(res.isExists());
        assertEquals("unicode-text-utf8.txt", res.getName());
    }

    @Test
    public void testGetInMemoryResource() throws Exception {
        final Resource res = registry.createResource(new SimpleResourceProperties("mem:///foo.bar.txt"));
        assertTrue(res instanceof InMemoryResource);
    }

    @Test
    public void testGetClasspathResource() throws Exception {
        final Resource res = registry.createResource(new SimpleResourceProperties("classpath:///folder/foo"));
        assertTrue(res.isExists());
        assertTrue(res instanceof ClasspathResource);
    }

    @Test
    public void testGetUrlResource() throws Exception {
        final Resource res = registry.createResource(new SimpleResourceProperties(
                "http://metamodel.apache.org/robots.txt"));
        assertTrue(res.isExists());
        assertTrue(res instanceof UrlResource);
    }
}
