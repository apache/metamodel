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
package org.eobjects.metamodel.query;

import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.TableType;
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
    }

    @Test
    public void testCloneWithParameterValues() {
        DefaultCompiledQuery defaultCompiledQuery = new DefaultCompiledQuery(query);
        Query resultQuery = defaultCompiledQuery.cloneWithParameterValues(new Object[] { "BE", 1, "BE" });

        defaultCompiledQuery = new DefaultCompiledQuery(resultQuery);
        Assert.assertEquals(0, defaultCompiledQuery.getParameters().size());
    }

}
