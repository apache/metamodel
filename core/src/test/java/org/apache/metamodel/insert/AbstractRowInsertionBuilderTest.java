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
package org.apache.metamodel.insert;

import static org.junit.Assert.assertEquals;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableTable;
import org.junit.Test;

public class AbstractRowInsertionBuilderTest {

    @Test
    public void testToString() {
        final MutableTable table = new MutableTable("tbl");
        final MutableColumn col1 = new MutableColumn("col1").setTable(table);
        final MutableColumn col2 = new MutableColumn("col2").setTable(table);
        table.addColumn(col1).addColumn(col2);

        final AbstractRowInsertionBuilder<UpdateCallback> builder = new AbstractRowInsertionBuilder<UpdateCallback>(
                null, table) {
            @Override
            public void execute() throws MetaModelException {
                throw new UnsupportedOperationException();
            }
        };

        builder.value(col1, "value1").value(col2, "value2");
        assertEquals("INSERT INTO tbl(col1,col2) VALUES (\"value1\",\"value2\")", builder.toString());
    }
}
