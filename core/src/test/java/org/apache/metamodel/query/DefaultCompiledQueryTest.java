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
package org.apache.metamodel.query;

import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.TableType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DefaultCompiledQueryTest {

    Query query;

    @Before
    public void setup() {
        query = new Query();

        MutableTable datastoreTable = new MutableTable("datastore", TableType.TABLE);

        MutableColumn dataSourceIdColumn = new MutableColumn("dataSourceIdColumn", ColumnType.VARCHAR);
        MutableColumn dataSourceNameColumn = new MutableColumn("dataSourceNameColumn", ColumnType.VARCHAR);
        MutableColumn versionColumn = new MutableColumn("versionColumn", ColumnType.INTEGER);
        MutableColumn changeSetColumn = new MutableColumn("changeSetColumn", ColumnType.INTEGER);

        SelectItem countSelectItem = new SelectItem(FunctionType.COUNT, dataSourceIdColumn);
        SelectItem dsIdSelectItem = new SelectItem(dataSourceIdColumn).setAlias("innerDataSourceRecordId");
        Query leftQuery = new Query();
        leftQuery.select(dsIdSelectItem);
        leftQuery.groupBy(dataSourceIdColumn);
        leftQuery.having(new FilterItem(countSelectItem.toSql() + " " + OperatorType.EQUALS_TO.toSql() + " 2"));
        leftQuery.where(dataSourceNameColumn, OperatorType.EQUALS_TO, new QueryParameter());
        leftQuery.from(datastoreTable);
        FromItem leftFrom = new FromItem(leftQuery);
        leftFrom.setAlias("innerDS");

        query.select(changeSetColumn);
        query.from(leftFrom, new FromItem(datastoreTable));
        query.where(versionColumn, OperatorType.EQUALS_TO, 2);
        query.where(changeSetColumn, OperatorType.EQUALS_TO, new QueryParameter());
        // Checks if max count is 2 in order to assert that this record has not
        // been a part of any changeSets previously and not processed by GR
        // creation in the current run.
        query.where(new SelectItem(dsIdSelectItem, leftFrom), OperatorType.EQUALS_TO, dsIdSelectItem);
        query.where(dataSourceNameColumn, OperatorType.EQUALS_TO, new QueryParameter());
    }

    @Test
    public void testGetParameterLogic() {

        DefaultCompiledQuery defaultCompiledQuery = new DefaultCompiledQuery(query);
        Assert.assertEquals(3, defaultCompiledQuery.getParameters().size());

        Assert.assertEquals(
                "DefaultCompiledQuery["
                        + "SELECT changeSetColumn FROM (SELECT dataSourceIdColumn AS innerDataSourceRecordId FROM datastore WHERE dataSourceNameColumn = ? GROUP BY dataSourceIdColumn HAVING COUNT(dataSourceIdColumn) = 2) innerDS, datastore "
                        + "WHERE versionColumn = 2 AND changeSetColumn = ? AND innerDS.innerDataSourceRecordId = dataSourceIdColumn AND dataSourceNameColumn = ?]",
                defaultCompiledQuery.toString());
        
        defaultCompiledQuery.close();
    }

    @Test
    public void testCloneWithParameterValues() {
        DefaultCompiledQuery defaultCompiledQuery = new DefaultCompiledQuery(query);
        Query resultQuery = defaultCompiledQuery.cloneWithParameterValues(new Object[] { "BE", 1, "BE" });
        defaultCompiledQuery.close();

        defaultCompiledQuery = new DefaultCompiledQuery(resultQuery);
        Assert.assertEquals(0, defaultCompiledQuery.getParameters().size());
        defaultCompiledQuery.close();
    }

}
