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
package org.eobjects.metamodel.create;

import junit.framework.TestCase;

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.MutableRef;

public class AbstractCreateTableBuilderTest extends TestCase {

    public void testExecute() throws Exception {
        final MutableRef<Boolean> executed = new MutableRef<Boolean>(false);

        Schema schema = new MutableSchema("schema");
        AbstractTableCreationBuilder<UpdateCallback> builder = new AbstractTableCreationBuilder<UpdateCallback>(null,
                schema, "tablename") {
            @Override
            public Table execute() throws MetaModelException {
                executed.set(true);
                return getTable();
            }
        };

        assertFalse(executed.get().booleanValue());

        builder.withColumn("foo").ofType(ColumnType.VARCHAR).asPrimaryKey().ofNativeType("vch").ofSize(1234)
                .nullable(true);
        builder.withColumn("bar").withColumn("baz").nullable(false);
        Table table = builder.execute();

        assertTrue(executed.get().booleanValue());

        assertEquals("tablename", table.getName());
        assertEquals(3, table.getColumnCount());
        assertEquals(
                "Column[name=foo,columnNumber=0,type=VARCHAR,nullable=true,nativeType=vch,columnSize=1234]",
                table.getColumns()[0].toString());
        assertEquals(
                "Column[name=bar,columnNumber=1,type=null,nullable=null,nativeType=null,columnSize=null]",
                table.getColumns()[1].toString());
        assertEquals(
                "Column[name=baz,columnNumber=2,type=null,nullable=false,nativeType=null,columnSize=null]",
                table.getColumns()[2].toString());

        assertEquals("CREATE TABLE schema.tablename (foo VARCHAR(1234) PRIMARY KEY,bar,baz NOT NULL)", builder.toSql());
    }

    public void testLike() throws Exception {
        final MutableRef<Boolean> executed = new MutableRef<Boolean>(false);

        Schema schema = new MutableSchema("schema");
        AbstractTableCreationBuilder<UpdateCallback> builder = new AbstractTableCreationBuilder<UpdateCallback>(null,
                schema, "tablename") {
            @Override
            public Table execute() throws MetaModelException {
                executed.set(true);
                return toTable();
            }
        };

        assertFalse(executed.get().booleanValue());

        MutableTable likeTable = new MutableTable("blablablabla");
        likeTable.addColumn(new MutableColumn("foo", ColumnType.VARCHAR, likeTable, 0, 1234, "vch", true, null, false,
                null));
        likeTable.addColumn(new MutableColumn("bar"));
        likeTable.addColumn(new MutableColumn("baz"));

        builder.like(likeTable);
        Table table = builder.execute();

        assertTrue(executed.get().booleanValue());

        assertEquals("tablename", table.getName());
        assertEquals(3, table.getColumnCount());
        assertEquals(
                "Column[name=foo,columnNumber=0,type=VARCHAR,nullable=true,nativeType=vch,columnSize=1234]",
                table.getColumns()[0].toString());
        assertEquals(
                "Column[name=bar,columnNumber=1,type=null,nullable=null,nativeType=null,columnSize=null]",
                table.getColumns()[1].toString());
        assertEquals(
                "Column[name=baz,columnNumber=2,type=null,nullable=null,nativeType=null,columnSize=null]",
                table.getColumns()[2].toString());
    }
}
