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
package org.apache.metamodel.create;

import junit.framework.TestCase;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.MutableRef;

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
        assertEquals("Column[name=foo,columnNumber=0,type=VARCHAR,nullable=true,nativeType=vch,columnSize=1234]",
                table.getColumns()[0].toString());
        assertEquals("Column[name=bar,columnNumber=1,type=null,nullable=null,nativeType=null,columnSize=null]",
                table.getColumns()[1].toString());
        assertEquals("Column[name=baz,columnNumber=2,type=null,nullable=false,nativeType=null,columnSize=null]",
                table.getColumns()[2].toString());

        System.out.println(builder.toSql());
        assertEquals("CREATE TABLE schema.tablename (foo VARCHAR(1234),bar,baz NOT NULL, PRIMARY KEY(foo))", builder.toSql());
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
                null).setPrimaryKey(true));
        likeTable.addColumn(new MutableColumn("bar"));
        likeTable.addColumn(new MutableColumn("baz"));

        builder.like(likeTable);
        Table table = builder.execute();

        assertTrue(executed.get().booleanValue());

        assertEquals("tablename", table.getName());
        assertEquals(3, table.getColumnCount());
        assertEquals("Column[name=foo,columnNumber=0,type=VARCHAR,nullable=true,nativeType=vch,columnSize=1234]",
                table.getColumns()[0].toString());
        assertTrue(table.getColumns()[0].isPrimaryKey());

        assertEquals("Column[name=bar,columnNumber=1,type=null,nullable=null,nativeType=null,columnSize=null]",
                table.getColumns()[1].toString());
        assertEquals("Column[name=baz,columnNumber=2,type=null,nullable=null,nativeType=null,columnSize=null]",
                table.getColumns()[2].toString());
    }
}
