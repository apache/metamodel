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
package org.apache.metamodel.query.parser;

import java.util.List;

import junit.framework.TestCase;

public class QueryPartParserTest extends TestCase {

    public void testParseNone() throws Exception {
        QueryPartCollectionProcessor itemParser = new QueryPartCollectionProcessor();

        new QueryPartParser(itemParser, " ", ",").parse();

        List<String> items = itemParser.getTokens();
        assertEquals(0, items.size());
        assertEquals("[]", items.toString());
    }

    public void testParseSingle() throws Exception {
        QueryPartCollectionProcessor itemParser = new QueryPartCollectionProcessor();

        new QueryPartParser(itemParser, "foo ", ",").parse();

        List<String> items = itemParser.getTokens();
        assertEquals(1, items.size());
        assertEquals("[foo]", items.toString());
        assertEquals("[null]", itemParser.getDelims().toString());
    }

    public void testParseMultiple() throws Exception {
        QueryPartCollectionProcessor itemParser = new QueryPartCollectionProcessor();

        new QueryPartParser(itemParser, "foo , bar", ",").parse();

        List<String> items = itemParser.getTokens();
        assertEquals(2, items.size());
        assertEquals("[foo, bar]", items.toString());
    }

    public void testParseWithParenthesis() throws Exception {
        QueryPartCollectionProcessor itemParser = new QueryPartCollectionProcessor();

        new QueryPartParser(itemParser, "foo , bar (a,b,c),(doh)", ",").parse();

        List<String> items = itemParser.getTokens();
        assertEquals("[foo, bar (a,b,c), (doh)]", items.toString());
        assertEquals(3, items.size());
    }

    public void testMultipleDelims() throws Exception {
        QueryPartCollectionProcessor itemParser = new QueryPartCollectionProcessor();

        new QueryPartParser(itemParser, "foo AND bar OR baz AND (foo( AND bar) OR baz)", " AND ", " OR ").parse();

        List<String> items = itemParser.getTokens();
        assertEquals(4, items.size());
        assertEquals("[foo, bar, baz, (foo( AND bar) OR baz)]", items.toString());
        assertEquals("[null,  AND ,  OR ,  AND ]", itemParser.getDelims().toString());
    }

    public void testEmptyClause() throws Exception {
        QueryPartCollectionProcessor itemParser = new QueryPartCollectionProcessor();

        new QueryPartParser(itemParser, "", ",").parse();
        assertEquals(0, itemParser.getTokens().size());
    }

    public void testEmptyParenthesis() throws Exception {
        QueryPartCollectionProcessor itemParser = new QueryPartCollectionProcessor();

        new QueryPartParser(itemParser, "()", ",").parse();
        assertEquals(0, itemParser.getTokens().size());
    }
    
    public void testMultiParenthesisLevels() throws Exception {
        QueryPartCollectionProcessor itemParser = new QueryPartCollectionProcessor();

        new QueryPartParser(itemParser, "(((Hello world)))", ",").parse();
        assertEquals(1, itemParser.getTokens().size());
        assertEquals("Hello world", itemParser.getTokens().get(0));
    }

    public void testOuterParenthesis() throws Exception {
        QueryPartCollectionProcessor itemParser = new QueryPartCollectionProcessor();

        new QueryPartParser(itemParser, "(foo,bar)", ",").parse();

        List<String> items = itemParser.getTokens();
        assertEquals(2, items.size());
        assertEquals("[foo, bar]", items.toString());
    }
}
