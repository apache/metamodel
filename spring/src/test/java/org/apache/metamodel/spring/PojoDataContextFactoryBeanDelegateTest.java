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
package org.apache.metamodel.spring;

import java.util.Arrays;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.pojo.PojoDataContext;
import org.apache.metamodel.schema.Schema;

import junit.framework.TestCase;

public class PojoDataContextFactoryBeanDelegateTest extends TestCase {

    public void testConvertTableDefs() throws Exception {
        final DataContextFactoryBean factoryBean = new DataContextFactoryBean();
        factoryBean.setType("POJO");
        factoryBean.setDatabaseName("my db");
        factoryBean.setTableDefinitions("hello world (greeting VARCHAR, who VARCHAR); foo (bar INTEGER, baz DATE);");

        final DataContext dataContext = factoryBean.getObject();
        assertTrue(dataContext instanceof PojoDataContext);

        Schema schema = dataContext.getDefaultSchema();
        assertEquals("my db", schema.getName());
        assertEquals("[foo, hello world]", Arrays.toString(schema.getTableNames()));

        assertEquals(
                "[Column[name=greeting,columnNumber=0,type=VARCHAR,nullable=true,nativeType=null,columnSize=null], "
                        + "Column[name=who,columnNumber=1,type=VARCHAR,nullable=true,nativeType=null,columnSize=null]]",
                Arrays.toString(schema.getTableByName("hello world").getColumns()));

        assertEquals("[Column[name=bar,columnNumber=0,type=INTEGER,nullable=true,nativeType=null,columnSize=null], "
                + "Column[name=baz,columnNumber=1,type=DATE,nullable=true,nativeType=null,columnSize=null]]",
                Arrays.toString(schema.getTableByName("foo").getColumns()));
    }
}
