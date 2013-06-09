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
package org.eobjects.metamodel.access;

import java.util.Arrays;
import java.util.Date;

import junit.framework.TestCase;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

public class AccessDataContextTest extends TestCase {

    private DataContext dc = new AccessDataContext("src/test/resources/developers.mdb");

    public void testDeveloperTable() throws Exception {
        Schema schema = dc.getDefaultSchema();
        assertEquals("developers.mdb", schema.getName());

        assertEquals("[developer, product]", Arrays.toString(schema.getTableNames()));

        Table table = schema.getTableByName("developer");
        assertEquals("[id, name, email, male, developer_since]", Arrays.toString(table.getColumnNames()));
        
        Column[] primaryKeys = table.getPrimaryKeys();
        assertEquals("[Column[name=id,columnNumber=0,type=INTEGER,nullable=null,nativeType=LONG,columnSize=4]]", Arrays.toString(primaryKeys));

        Column nameCol = table.getColumnByName("name");
        assertEquals(
                "Column[name=name,columnNumber=1,type=VARCHAR,nullable=null,nativeType=TEXT,columnSize=100]",
                nameCol.toString());

        Column maleCol = table.getColumnByName("male");
        assertEquals(
                "Column[name=male,columnNumber=3,type=BOOLEAN,nullable=null,nativeType=BOOLEAN,columnSize=1]",
                maleCol.toString());

        Column developerSinceCol = table.getColumnByName("developer_since");
        assertEquals(
                "Column[name=developer_since,columnNumber=4,type=TIMESTAMP,nullable=null,nativeType=SHORT_DATE_TIME,columnSize=8]",
                developerSinceCol.toString());

        DataSet ds = dc.executeQuery(new Query().select(nameCol, maleCol, developerSinceCol).from(table));
        while (ds.next()) {
            Row row = ds.getRow();
            assertEquals(3, row.getValues().length);
            Object value = row.getValue(0);
            assertEquals(String.class, value.getClass());
            value = row.getValue(1);
            assertEquals(Boolean.class, value.getClass());
            value = row.getValue(2);
            assertTrue(value instanceof Date);
        }
    }

    public void testProductTable() throws Exception {
        Schema schema = dc.getDefaultSchema();
        assertEquals("developers.mdb", schema.getName());

        Table table = schema.getTableByName("product");
        assertEquals("[id, name, version, founder_developer]", Arrays.toString(table.getColumnNames()));

        Column idCol = table.getColumnByName("id");
        assertEquals(
                "Column[name=id,columnNumber=0,type=INTEGER,nullable=null,nativeType=LONG,columnSize=4]",
                idCol.toString());

        Column nameCol = table.getColumnByName("name");
        assertEquals(
                "Column[name=name,columnNumber=1,type=VARCHAR,nullable=null,nativeType=TEXT,columnSize=100]",
                nameCol.toString());

        Column versionCol = table.getColumnByName("version");
        assertEquals(
                "Column[name=version,columnNumber=2,type=INTEGER,nullable=null,nativeType=LONG,columnSize=4]",
                versionCol.toString());

        Column founderCol = table.getColumnByName("founder_developer");
        assertEquals(
                "Column[name=founder_developer,columnNumber=3,type=INTEGER,nullable=null,nativeType=LONG,columnSize=4]",
                founderCol.toString());

        DataSet ds;

        ds = dc.executeQuery(new Query().select(nameCol, versionCol, founderCol).from(table));
        assertTrue(ds.next());
        assertEquals("Anthons Algorithms", ds.getRow().getValue(nameCol).toString());
        assertEquals(11, ds.getRow().getValue(versionCol));
        assertEquals(1, ds.getRow().getValue(founderCol));
        assertTrue(ds.next());
        assertEquals("Barbaras Basic Bundle", ds.getRow().getValue(nameCol).toString());
        assertEquals(2, ds.getRow().getValue(versionCol));
        assertEquals(2, ds.getRow().getValue(founderCol));
        assertFalse(ds.next());
        ds.close();

        ds = dc.query().from(table).selectCount().execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[2]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();
    }
}