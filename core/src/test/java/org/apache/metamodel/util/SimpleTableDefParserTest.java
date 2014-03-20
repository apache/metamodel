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

import org.apache.metamodel.schema.ColumnType;

public class SimpleTableDefParserTest extends TestCase {

    public void testParseTableDefNoExtraWhitespace() throws Exception {
        SimpleTableDef tableDef = SimpleTableDefParser.parseTableDef("foo(bar VARCHAR,baz INTEGER)");
        assertNotNull(tableDef);
        assertEquals("foo", tableDef.getName());
        assertEquals(2, tableDef.getColumnNames().length);
        assertEquals("bar", tableDef.getColumnNames()[0]);
        assertEquals("baz", tableDef.getColumnNames()[1]);
        assertEquals(ColumnType.VARCHAR, tableDef.getColumnTypes()[0]);
        assertEquals(ColumnType.INTEGER, tableDef.getColumnTypes()[1]);
    }

    public void testParseTableDefWithExtraWhitespace() throws Exception {
        SimpleTableDef tableDef = SimpleTableDefParser
                .parseTableDef("   foo ( bar   VARCHAR   ,    baz    INTEGER    )   ");
        assertNotNull(tableDef);
        assertEquals("foo", tableDef.getName());
        assertEquals(2, tableDef.getColumnNames().length);
        assertEquals("bar", tableDef.getColumnNames()[0]);
        assertEquals("baz", tableDef.getColumnNames()[1]);
        assertEquals(ColumnType.VARCHAR, tableDef.getColumnTypes()[0]);
        assertEquals(ColumnType.INTEGER, tableDef.getColumnTypes()[1]);
    }

    public void testParseTableDefsGibberish() throws Exception {
        try {
            SimpleTableDefParser.parseTableDefs(" lorem ipsum gibberish ");
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertEquals(
                    "Failed to parse table definition:  lorem ipsum gibberish . No start parenthesis found for column section.",
                    e.getMessage());
        }
    }

    public void testParseTableDefs() throws Exception {
        SimpleTableDef[] tableDefs = SimpleTableDefParser
                .parseTableDefs("   foo ( bar   MAP   ,    baz    OTHER    )  ; \n\n\r\n\t  hello (  world   BINARY    )  ");

        assertNotNull(tableDefs);
        assertEquals(2, tableDefs.length);

        SimpleTableDef tableDef = tableDefs[0];
        assertNotNull(tableDef);
        assertEquals("foo", tableDef.getName());
        assertEquals(2, tableDef.getColumnNames().length);
        assertEquals("bar", tableDef.getColumnNames()[0]);
        assertEquals("baz", tableDef.getColumnNames()[1]);
        assertEquals(ColumnType.MAP, tableDef.getColumnTypes()[0]);
        assertEquals(ColumnType.OTHER, tableDef.getColumnTypes()[1]);

        tableDef = tableDefs[1];
        assertNotNull(tableDef);
        assertEquals("hello", tableDef.getName());
        assertEquals(1, tableDef.getColumnNames().length);
        assertEquals("world", tableDef.getColumnNames()[0]);
        assertEquals(ColumnType.BINARY, tableDef.getColumnTypes()[0]);
    }
}
