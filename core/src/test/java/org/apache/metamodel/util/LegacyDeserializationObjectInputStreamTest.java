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

import java.io.FileInputStream;

import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Table;

import junit.framework.TestCase;

public class LegacyDeserializationObjectInputStreamTest extends TestCase {

    /**
     * Method used to generate the input file (requires org.eobjects.metamodel
     * available)
     * 
     * <pre>
     * final org.apache.metamodel.schema.MutableSchema schema = new org.apache.metamodel.schema.MutableSchema(&quot;myschema&quot;);
     * final org.apache.metamodel.schema.MutableTable table = new org.apache.metamodel.schema.MutableTable(&quot;mytable&quot;,
     *         org.apache.metamodel.schema.TableType.TABLE, schema);
     * schema.addTable(table);
     * 
     * table.addColumn(new org.apache.metamodel.schema.MutableColumn(&quot;mycol1&quot;, org.apache.metamodel.schema.ColumnType.INTEGER,
     *         table, 0, 16, &quot;int&quot;, false, &quot;my remark 1&quot;, false, &quot;\&quot;&quot;));
     * table.addColumn(new org.apache.metamodel.schema.MutableColumn(&quot;mycol1&quot;, org.apache.metamodel.schema.ColumnType.VARCHAR,
     *         table, 1, 255, &quot;text&quot;, true, &quot;my remark 2&quot;, true, null));
     * 
     * final org.apache.metamodel.query.Query q = new org.apache.metamodel.query.Query();
     * q.from(table);
     * q.select(table.getColumn(0));
     * q.where(table.getColumn(1), org.apache.metamodel.query.OperatorType.EQUALS_TO, &quot;foo&quot;);
     * 
     * final FileOutputStream out = new FileOutputStream(filename);
     * try {
     *     new ObjectOutputStream(out).writeObject(q);
     * } finally {
     *     out.close();
     * }
     * </pre>
     */
    public void testCreateSerializeAndDeserializeOldSchemaAndQuery() throws Exception {
        final String filename = "src/test/resources/metamodel-3.4-query-and-schema.ser";

        final Object obj;
        {
            final FileInputStream in = new FileInputStream(filename);
            try {
                final LegacyDeserializationObjectInputStream ois = new LegacyDeserializationObjectInputStream(in);
                obj = ois.readObject();
                ois.close();
            } finally {
                in.close();
            }
        }

        assertTrue(obj instanceof Query);

        final Query q = (Query) obj;
        final Table table = q.getFromClause().getItem(0).getTable();

        assertEquals("Table[name=mytable,type=TABLE,remarks=null]", table.toString());
        assertEquals("Column[name=mycol1,columnNumber=0,type=INTEGER,nullable=false,nativeType=int,columnSize=16]",
                table.getColumn(0).toString());
        assertEquals("Column[name=mycol1,columnNumber=1,type=VARCHAR,nullable=true,nativeType=text,columnSize=255]",
                table.getColumn(1).toString());

        assertEquals("SELECT mytable.\"mycol1\" FROM myschema.mytable WHERE mytable.mycol1 = 'foo'", q.toSql());
    }
}