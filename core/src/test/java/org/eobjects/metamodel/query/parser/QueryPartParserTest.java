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
package org.eobjects.metamodel.query.parser;

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
