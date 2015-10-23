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

import org.apache.metamodel.MetaModelTestCase;
import org.apache.metamodel.query.OrderByItem.Direction;
import org.apache.metamodel.query.builder.InitFromBuilderImpl;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.TableType;

public class QueryTest extends MetaModelTestCase {

    private Schema _schema = getExampleSchema();

    public void testSimpleQuery() throws Exception {
        Table contributorTable = _schema.getTableByName(TABLE_CONTRIBUTOR);

        Query q = new Query();
        q.selectCount().from(contributorTable);
        assertEquals("SELECT COUNT(*) FROM MetaModelSchema.contributor", q.toString());
    }

    public void testCloneGroupBy() throws Exception {
        Table table = _schema.getTableByName(TABLE_PROJECT);
        Column column = table.getColumnByName(COLUMN_PROJECT_NAME);
        Query q = new Query().from(table).selectCount().select(column).groupBy(column);
        assertEquals(q.toString(), q.clone().toString());

        q.having(new FilterItem(SelectItem.getCountAllItem(), OperatorType.GREATER_THAN, 20));
        assertEquals(q.toString(), q.clone().toString());
    }

    public void testFromItemAlias() throws Exception {
        Query q = new Query();
        Table contributorTable = _schema.getTableByName(TABLE_CONTRIBUTOR);
        Column nameColumn = contributorTable.getColumnByName(COLUMN_CONTRIBUTOR_NAME);
        Column countryColumn = contributorTable.getColumnByName(COLUMN_CONTRIBUTOR_COUNTRY);

        FromItem fromContributor = new FromItem(contributorTable);
        q.from(fromContributor);
        q.select(nameColumn, countryColumn);
        assertEquals("SELECT contributor.name, contributor.country FROM MetaModelSchema.contributor", q.toString());

        fromContributor.setAlias("c");

        assertEquals("SELECT c.name, c.country FROM MetaModelSchema.contributor c", q.toString());

        q.groupBy(new GroupByItem(q.getSelectClause().getSelectItem(nameColumn)));
        q.groupBy(new GroupByItem(q.getSelectClause().getSelectItem(countryColumn)));
        q.select(new SelectItem(FunctionType.COUNT, "*", "total"));
        assertEquals(2, q.getGroupByClause().getItems().size());
        assertEquals(
                "SELECT c.name, c.country, COUNT(*) AS total FROM MetaModelSchema.contributor c GROUP BY c.name, c.country",
                q.toString());

        Column contributorIdColumn = contributorTable.getColumnByName(COLUMN_CONTRIBUTOR_CONTRIBUTOR_ID);
        q.where(contributorIdColumn, OperatorType.EQUALS_TO, 1);
        assertEquals(
                "SELECT c.name, c.country, COUNT(*) AS total FROM MetaModelSchema.contributor c WHERE c.contributor_id = 1 GROUP BY c.name, c.country",
                q.toString());

        q.where(contributorIdColumn, OperatorType.DIFFERENT_FROM, q.getSelectClause().getSelectItem(nameColumn));
        assertEquals(
                "SELECT c.name, c.country, COUNT(*) AS total FROM MetaModelSchema.contributor c WHERE c.contributor_id = 1 AND c.contributor_id <> c.name GROUP BY c.name, c.country",
                q.toString());
    }

    public void testAddOrderBy() throws Exception {
        Table contributorTable = _schema.getTableByName(TABLE_CONTRIBUTOR);
        Column nameColumn = contributorTable.getColumnByName(COLUMN_CONTRIBUTOR_NAME);
        Column countryColumn = contributorTable.getColumnByName(COLUMN_CONTRIBUTOR_COUNTRY);
        FromItem fromContributor = new FromItem(contributorTable);
        fromContributor.setAlias("a");

        Query q = new Query();
        q.select(nameColumn, countryColumn).from(fromContributor).orderBy(nameColumn)
                .orderBy(countryColumn, Direction.DESC);
        assertEquals(2, q.getOrderByClause().getItems().size());
        assertEquals("SELECT a.name, a.country FROM MetaModelSchema.contributor a ORDER BY a.name ASC, a.country DESC",
                q.toString());
    }

    public void testCloneJoinAndOrderBy() throws Exception {
        Query q1 = new Query();
        Table contributorTable = _schema.getTableByName(TABLE_CONTRIBUTOR);
        Table roleTable = _schema.getTableByName(TABLE_ROLE);
        FromItem fromItem = new FromItem(JoinType.INNER, contributorTable.getRelationships(roleTable)[0]);
        q1.from(fromItem);

        Column nameColumn = contributorTable.getColumnByName(COLUMN_CONTRIBUTOR_NAME);
        Column countryColumn = contributorTable.getColumnByName(COLUMN_CONTRIBUTOR_COUNTRY);
        Column roleNameColumn = roleTable.getColumnByName(COLUMN_ROLE_ROLE_NAME);
        q1.select(nameColumn, countryColumn, roleNameColumn);
        q1.orderBy(roleNameColumn);
        String q1string = q1.toString();
        assertEquals(
                "SELECT contributor.name, contributor.country, role.name FROM MetaModelSchema.contributor INNER JOIN MetaModelSchema.role ON contributor.contributor_id = role.contributor_id ORDER BY role.name ASC",
                q1string);

        Query q2 = q1.clone();
        assertEquals(q1string, q2.toString());

        q2.getSelectClause().removeItem(1);
        assertEquals(q1string, q1.toString());
        assertEquals(
                "SELECT contributor.name, role.name FROM MetaModelSchema.contributor INNER JOIN MetaModelSchema.role ON contributor.contributor_id = role.contributor_id ORDER BY role.name ASC",
                q2.toString());

        FromItem sqFromItem = new FromItem(q2).setAlias("sq");
        SelectItem sqSelectItem = new SelectItem(q2.getSelectClause().getItem(1), sqFromItem).setAlias("foo");
        Query q3 = new Query().from(sqFromItem);
        q3.orderBy(new OrderByItem(sqSelectItem));
        q3.select(sqSelectItem);
        assertEquals(
                "SELECT sq.name AS foo FROM (SELECT contributor.name, role.name FROM MetaModelSchema.contributor INNER JOIN MetaModelSchema.role ON contributor.contributor_id = role.contributor_id ORDER BY role.name ASC) sq ORDER BY foo ASC",
                q3.toString());
        Query q4 = q3.clone();
        assertEquals(
                "SELECT sq.name AS foo FROM (SELECT contributor.name, role.name FROM MetaModelSchema.contributor INNER JOIN MetaModelSchema.role ON contributor.contributor_id = role.contributor_id ORDER BY role.name ASC) sq ORDER BY foo ASC",
                q4.toString());

        assertTrue(q3.equals(q4));
    }

    public void testDistinctEquals() throws Exception {
        Query q = new Query();
        SelectClause sc1 = new SelectClause(q);
        SelectClause sc2 = new SelectClause(q);
        assertTrue(sc1.equals(sc2));
        sc2.setDistinct(true);
        assertFalse(sc1.equals(sc2));
        sc1.setDistinct(true);
        assertTrue(sc1.equals(sc2));
    }

    public void testSetMaxRows() throws Exception {
        assertEquals(1, new Query().setMaxRows(1).getMaxRows().intValue());
        try {
            new Query().setMaxRows(-1);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertEquals("Max rows cannot be negative", e.getMessage());
        }
    }

    public void testSetFirstRow() throws Exception {
        assertEquals(2, new Query().setFirstRow(2).getFirstRow().intValue());
        assertEquals(1, new Query().setFirstRow(1).getFirstRow().intValue());

        try {
            new Query().setFirstRow(0);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertEquals("First row cannot be negative or zero", e.getMessage());
        }

        try {
            new Query().setFirstRow(-1);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertEquals("First row cannot be negative or zero", e.getMessage());
        }
    }

    public void testEqualsAndHashCode() throws Exception {
        MutableSchema schema = new MutableSchema("schema");
        MutableTable table = new MutableTable("table").setSchema(schema);
        schema.addTable(table);

        Column col1 = new MutableColumn("col1").setTable(table);
        Column col2 = new MutableColumn("col2").setTable(table);
        Column col3 = new MutableColumn("col3").setTable(table);
        table.addColumn(col1);
        table.addColumn(col2);
        table.addColumn(col3);

        Query q1 = new Query().select(col1, col2).from(table).where(col3, OperatorType.EQUALS_TO, "m'jello");

        Query q2 = new InitFromBuilderImpl(null).from(table).select(col1).and(col2).where(col3).eq("m'jello").toQuery();

        assertEquals(q1, q2);
    }

    public void testHavingClauseReferencingFunctionAndOperand() throws Exception {
        MutableColumn idColumn = new MutableColumn("id", ColumnType.VARCHAR);
        SelectItem countSelectItem = new SelectItem(FunctionType.COUNT, idColumn);
        SelectItem idSelectItem = new SelectItem(idColumn).setAlias("innerIdColumn");

        Query q = new Query();
        q.select(idSelectItem);
        q.groupBy(idColumn);
        q.having(new FilterItem(countSelectItem, OperatorType.EQUALS_TO, 2));

        assertEquals("SELECT id AS innerIdColumn GROUP BY id HAVING COUNT(id) = 2", q.toSql());
    }

    public void testToSqlWithFullyQualifiedColumnNames() throws Exception {
        final MutableSchema schema = new MutableSchema("sch");
        final MutableTable table = new MutableTable("tab", TableType.TABLE, schema);
        final MutableColumn nameColumn = new MutableColumn("name", ColumnType.VARCHAR).setTable(table);
        final MutableColumn ageColumn = new MutableColumn("age", ColumnType.INTEGER).setTable(table);
        schema.addTable(table);
        table.addColumn(nameColumn);
        table.addColumn(ageColumn);

        final Query q = new Query();
        q.select(ageColumn).selectCount();
        q.from(table);
        q.where(ageColumn, OperatorType.GREATER_THAN, 18);
        q.groupBy(ageColumn);
        q.having(FunctionType.COUNT, nameColumn, OperatorType.LESS_THAN, 100);
        q.orderBy(ageColumn);

        assertEquals("SELECT sch.tab.age, COUNT(*) FROM sch.tab WHERE sch.tab.age > 18 "
                + "GROUP BY sch.tab.age HAVING COUNT(sch.tab.name) < 100 ORDER BY sch.tab.age ASC", q.toSql(true));
    }
}