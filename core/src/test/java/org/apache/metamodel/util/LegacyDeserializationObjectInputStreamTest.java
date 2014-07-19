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
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

import junit.framework.TestCase;

public class LegacyDeserializationObjectInputStreamTest extends TestCase {

    /**
     * Method initially used (with org.eobjects codebase) to generate the test
     * input file:
     * 
     * <pre>
     * public void testCreateOldSchema() throws Exception {
     *     MutableSchema schema = new MutableSchema(&quot;myschema&quot;);
     *     MutableTable table = new MutableTable(&quot;mytable&quot;, TableType.TABLE, schema);
     *     schema.addTable(table);
     * 
     *     table.addColumn(new MutableColumn(&quot;mycol1&quot;, ColumnType.INTEGER, table, 0, 16, &quot;int&quot;, false, &quot;my remark 1&quot;, false,
     *             &quot;\&quot;&quot;));
     *     table.addColumn(new MutableColumn(&quot;mycol1&quot;, ColumnType.VARCHAR, table, 1, 255, &quot;text&quot;, true, &quot;my remark 2&quot;, true,
     *             null));
     * 
     *     Query q = new Query();
     *     q.from(table);
     *     q.select(table.getColumn(0));
     *     q.where(table.getColumn(1), OperatorType.EQUALS_TO, &quot;foo&quot;);
     * 
     *     FileOutputStream out = new FileOutputStream(&quot;src/test/resources/metamodel-3.4-query-and-schema.ser&quot;);
     *     new ObjectOutputStream(out).writeObject(q);
     *     out.close();
     * }
     * </pre>
     */
    public void testDeserializeOldQueryAndSchema() throws Exception {
        final Object obj;
        {
            final FileInputStream in = new FileInputStream("src/test/resources/metamodel-3.4-query-and-schema.ser");
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
        final Column[] columns = table.getColumns();

        assertEquals("Table[name=mytable,type=TABLE,remarks=null]", table.toString());
        assertEquals("Column[name=mycol1,columnNumber=0,type=INTEGER,nullable=false,nativeType=int,columnSize=16]", columns[0].toString());
        assertEquals("Column[name=mycol1,columnNumber=1,type=VARCHAR,nullable=true,nativeType=text,columnSize=255]", columns[1].toString());
        
        assertEquals("SELECT mytable.\"mycol1\" FROM myschema.mytable WHERE mytable.mycol1 = 'foo'", q.toSql());
    }

}
