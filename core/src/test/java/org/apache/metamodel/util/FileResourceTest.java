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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileResourceTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testCannotWriteToDirectory() throws Exception {
        FileResource dir = new FileResource(".");
        assertTrue(dir.isReadOnly());

        try {
            dir.write();
            fail("Exception expected");
        } catch (ResourceException e) {
            assertEquals("Cannot write to directory: .", e.getMessage());
        }
    }

    @Test
    public void testSizeAndLastModifiedOfDirectory() throws Exception {
        final FileResource dir = new FileResource(".");
        assertTrue(dir.getLastModified() > 0);
        assertTrue(dir.getSize() > 10);
    }

    @Test
    public void testReadDirectory() throws Exception {
        final String contentString = "fun and games with Apache MetaModel and Hadoop is what we do";
        final String[] contents = new String[] { "fun ", "and ", "games ", "with ", "Apache ", "MetaModel ", "and ",
                "Hadoop ", "is ", "what ", "we ", "do" };

        // Reverse both filename and contents to make sure it is the name and
        // not the creation order that is sorted on.
        int i = contents.length;
        Collections.reverse(Arrays.asList(contents));
        for (final String contentPart : contents) {
            final FileResource partResource = new FileResource(folder.newFile("/part-" + String.format("%02d", i--)));
            partResource.write(new Action<OutputStream>() {
                @Override
                public void run(OutputStream out) throws Exception {
                    out.write(contentPart.getBytes());
                }
            });
        }

        final FileResource res1 = new FileResource(folder.getRoot());

        final String str1 = res1.read(in -> {
            return FileHelper.readInputStreamAsString(in, "UTF8");
        });

        Assert.assertEquals(contentString, str1);

        final String str2 = res1.read(in -> {
                return FileHelper.readInputStreamAsString(in, "UTF8");
        });
        Assert.assertEquals(str1, str2);
    }

}