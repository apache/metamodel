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
package org.apache.metamodel.jdbc.dialects;

import junit.framework.TestCase;

public class VersionParserTest extends TestCase {

    public void testGetVersion() throws Exception {
        String version = VersionParser.getVersion("10.11.0");
        assertEquals("10.11.0", version);
    }

    public void testGetVersionEmpty() throws Exception {
        String version = VersionParser.getVersion("string with out version");
        assertEquals("", version);
    }

    public void testGetVersionNull() throws Exception {
        String version = VersionParser.getVersion(null);
        assertEquals("", version);
    }

    public void testGetVersionWithLetters() throws Exception {
        String version = VersionParser.getVersion("test 10.11.0V test");
        assertEquals("10.11.0", version);
    }

    public void testGetVersionWithSpecialChars() throws Exception {
        String version = VersionParser.getVersion("!@#test. 10.11.0V .test");
        assertEquals("10.11.0", version);
    }

    public void testGetVersionWithLettersAndNumbers() throws Exception {
        String version = VersionParser.getVersion("10 55g test 10.11.0 test 100");
        assertEquals("10.11.0", version);
    }

    public void testGetVersionMultipleVersions() throws Exception {
        String version = VersionParser.getVersion("test 10.11.0 11.11 test");
        assertEquals("10.11.0", version);
    }

    public void testGetMajorVersion() throws Exception {
        int version = VersionParser.getMajorVersion("10.11.0");
        assertEquals(10, version);
    }

    public void testGetMajorVersionEmpty() throws Exception {
        int version = VersionParser.getMajorVersion("string with out version");
        assertEquals(-1, version);
    }

    public void testGetMajorVersionNull() throws Exception {
        int version = VersionParser.getMajorVersion(null);
        assertEquals(-1, version);
    }

    public void testGetMajorVersionWithLetters() throws Exception {
        int version = VersionParser.getMajorVersion("test 10.11.0V test");
        assertEquals(10, version);
    }

    public void testGetMajorVersionWithSpecialChars() throws Exception {
        int version = VersionParser.getMajorVersion("!@#test. .10.11.0V .test");
        assertEquals(10, version);
    }

    public void testGetMajorVersionWithLettersAndNumbers() throws Exception {
        int version = VersionParser.getMajorVersion("10 55g test 10.11.0 test 100");
        assertEquals(10, version);
    }

    public void testGetMajorVersionMultipleVersions() throws Exception {
        int version = VersionParser.getMajorVersion("test 10.11.0V 11.11 test");
        assertEquals(10, version);
    }
}
