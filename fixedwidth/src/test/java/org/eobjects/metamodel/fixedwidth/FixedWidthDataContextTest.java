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
package org.eobjects.metamodel.fixedwidth;

import java.io.File;
import java.util.Arrays;

import junit.framework.TestCase;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.fixedwidth.FixedWidthConfiguration;
import org.eobjects.metamodel.fixedwidth.FixedWidthDataContext;
import org.eobjects.metamodel.fixedwidth.InconsistentValueWidthException;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

public class FixedWidthDataContextTest extends TestCase {

    public void testEmptyFile() throws Exception {
        DataContext dc = new FixedWidthDataContext(new File("src/test/resources/empty_file.txt"),
                new FixedWidthConfiguration(10));
        assertEquals(1, dc.getDefaultSchema().getTableCount());

        Table table = dc.getDefaultSchema().getTables()[0];
        assertEquals("empty_file", table.getName());
        assertEquals(0, table.getColumnCount());
    }

    public void testEmptyFileNoHeaderLine() throws Exception {
        DataContext dc = new FixedWidthDataContext(new File("src/test/resources/empty_file.txt"),
                new FixedWidthConfiguration(FixedWidthConfiguration.NO_COLUMN_NAME_LINE, "UTF8", 10));
        assertEquals(1, dc.getDefaultSchema().getTableCount());

        Table table = dc.getDefaultSchema().getTables()[0];
        assertEquals("empty_file", table.getName());
        assertEquals(0, table.getColumnCount());
    }

    public void testUnexistingHeaderLine() throws Exception {
        DataContext dc = new FixedWidthDataContext(new File("src/test/resources/example_simple1.txt"),
                new FixedWidthConfiguration(20, "UTF8", 10));
        assertEquals(1, dc.getDefaultSchema().getTableCount());

        Table table = dc.getDefaultSchema().getTables()[0];
        assertEquals("example_simple1", table.getName());
        assertEquals(0, table.getColumnCount());
    }

    public void testExampleSimple1() throws Exception {
        FixedWidthConfiguration conf = new FixedWidthConfiguration(10);
        FixedWidthDataContext dc = new FixedWidthDataContext(new File("src/test/resources/example_simple1.txt"), conf);

        String[] schemaNames = dc.getSchemaNames();
        assertEquals(2, schemaNames.length);
        assertEquals("[information_schema, example_simple1.txt]", Arrays.toString(schemaNames));

        Schema schema = dc.getDefaultSchema();
        assertEquals("Schema[name=example_simple1.txt]", schema.toString());

        assertEquals(1, schema.getTableCount());

        Table table = schema.getTableByName("example_simple1");
        assertEquals("Table[name=example_simple1,type=TABLE,remarks=null]", table.toString());

        assertEquals("[greeting, greeter]", Arrays.toString(table.getColumnNames()));
        assertEquals(10, table.getColumnByName("greeting").getColumnSize().intValue());
        assertEquals(10, table.getColumnByName("greeter").getColumnSize().intValue());

        Query q = dc.query().from(table).select(table.getColumns()).toQuery();
        DataSet ds = dc.executeQuery(q);

        assertTrue(ds.next());
        assertEquals("[hello, world]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[hi, there]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[howdy, partner]", Arrays.toString(ds.getRow().getValues()));
        assertFalse(ds.next());
    }

    public void testFailOnInconsistentWidth() throws Exception {
        FixedWidthConfiguration conf = new FixedWidthConfiguration(FixedWidthConfiguration.NO_COLUMN_NAME_LINE, "UTF8",
                10, true);
        FixedWidthDataContext dc = new FixedWidthDataContext(new File("src/test/resources/example_simple1.txt"), conf);

        String[] schemaNames = dc.getSchemaNames();
        assertEquals(2, schemaNames.length);
        assertEquals("[information_schema, example_simple1.txt]", Arrays.toString(schemaNames));

        Schema schema = dc.getDefaultSchema();
        assertEquals("Schema[name=example_simple1.txt]", schema.toString());

        assertEquals(1, schema.getTableCount());

        Table table = schema.getTableByName("example_simple1");
        assertEquals("Table[name=example_simple1,type=TABLE,remarks=null]", table.toString());

        assertEquals("[A, B]", Arrays.toString(table.getColumnNames()));

        Query q = dc.query().from(table).select(table.getColumns()).toQuery();
        DataSet ds = dc.executeQuery(q);

        assertTrue(ds.next());
        assertEquals("greeting", ds.getRow().getValue(0));
        assertEquals("greeter", ds.getRow().getValue(1));

        try {
            ds.next();
            fail("Exception expected");
        } catch (InconsistentValueWidthException e) {
            assertEquals("Inconsistent row format of row no. 2.", e.getMessage());
            assertEquals("[hello, world]", Arrays.toString(e.getProposedRow().getValues()));
        }
        assertTrue(ds.next());
        assertEquals("[hi, there]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[howdy, partner]", Arrays.toString(ds.getRow().getValues()));
        assertFalse(ds.next());
    }

    public void testVaryingValueLengthsCorrect() throws Exception {
        DataContext dc = new FixedWidthDataContext(new File("src/test/resources/example_simple2.txt"),
                new FixedWidthConfiguration(new int[] { 1, 8, 7 }));
        Table table = dc.getDefaultSchema().getTables()[0];
        assertEquals("[i, greeting, greeter]", Arrays.toString(table.getColumnNames()));

        assertEquals(1, table.getColumnByName("i").getColumnSize().intValue());
        assertEquals(8, table.getColumnByName("greeting").getColumnSize().intValue());
        assertEquals(7, table.getColumnByName("greeter").getColumnSize().intValue());

        Query q = dc.query().from(table).select(table.getColumns()).toQuery();
        DataSet ds = dc.executeQuery(q);

        assertTrue(ds.next());
        assertEquals("[1, hello, world]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[2, hi, there]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[3, howdy, partner]", Arrays.toString(ds.getRow().getValues()));
        assertFalse(ds.next());
    }

    public void testVaryingValueLengthsTooShortLength() throws Exception {
        DataContext dc = new FixedWidthDataContext(new File("src/test/resources/example_simple2.txt"),
                new FixedWidthConfiguration(0, "UTF8", new int[] { 1, 5, 7 }, true));
        try {
            dc.getDefaultSchema().getTables();
            fail("Exception expected");
        } catch (InconsistentValueWidthException e) {
            assertEquals("Inconsistent row format of row no. 1.", e.getMessage());
            assertEquals("igreetinggreeter", e.getSourceLine());
            assertEquals("[i, greet, inggree]", Arrays.toString(e.getSourceResult()));
        }
    }

    public void testVaryingValueLengthsTooShortLengthErrorTolerant() throws Exception {
        DataContext dc = new FixedWidthDataContext(new File("src/test/resources/example_simple2.txt"),
                new FixedWidthConfiguration(FixedWidthConfiguration.DEFAULT_COLUMN_NAME_LINE, "UTF8", new int[] { 1, 5,
                        7 }, false));

        Table table = dc.getDefaultSchema().getTables()[0];
        assertEquals("[i, greet, inggree]", Arrays.toString(table.getColumnNames()));

        Query q = dc.query().from(table).select(table.getColumns()).toQuery();
        DataSet ds = dc.executeQuery(q);

        assertTrue(ds.next());
        assertEquals("[1, hello, worl]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[2, hi, ther]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[3, howdy, part]", Arrays.toString(ds.getRow().getValues()));
        assertFalse(ds.next());
    }

    public void testVaryingValueLengthsTooLongLength() throws Exception {
        DataContext dc = new FixedWidthDataContext(new File("src/test/resources/example_simple2.txt"),
                new FixedWidthConfiguration(0, "UTF8", new int[] { 1, 8, 9 }, true));

        try {
            dc.getDefaultSchema().getTables();
            fail("Exception expected");
        } catch (InconsistentValueWidthException e) {
            assertEquals("Inconsistent row format of row no. 1.", e.getMessage());
            assertEquals("igreetinggreeter", e.getSourceLine());
            assertEquals("[i, greeting, greeter]", Arrays.toString(e.getSourceResult()));
        }
    }

    public void testVaryingValueLengthsTooLongLengthErrorTolerant() throws Exception {
        DataContext dc = new FixedWidthDataContext(new File("src/test/resources/example_simple2.txt"),
                new FixedWidthConfiguration(FixedWidthConfiguration.DEFAULT_COLUMN_NAME_LINE, "UTF8", new int[] { 1, 8,
                        9 }, false));

        Table table = dc.getDefaultSchema().getTables()[0];
        assertEquals("[i, greeting, greeter]", Arrays.toString(table.getColumnNames()));

        Query q = dc.query().from(table).select(table.getColumns()).toQuery();
        DataSet ds = dc.executeQuery(q);

        assertTrue(ds.next());
        assertEquals("[1, hello, world]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[2, hi, there]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[3, howdy, partner]", Arrays.toString(ds.getRow().getValues()));
        assertFalse(ds.next());
    }
}
