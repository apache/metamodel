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
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

import java.util.List;

public class SelectItemTest extends MetaModelTestCase {

    private Schema _schema = getExampleSchema();

    public void testSelectColumnInFromItem() throws Exception {
        final Table projectTable = _schema.getTableByName(TABLE_PROJECT);
        final Column column1 = projectTable.getColumns().get(0);
        final Column column2 = projectTable.getColumns().get(1);

        Query q = new Query().from(projectTable, "a").from(projectTable, "b");
        q.select(column1, q.getFromClause().getItem(1));
        q.select(column2, q.getFromClause().getItem(0));

        assertEquals("SELECT b.project_id, a.name FROM MetaModelSchema.project a, MetaModelSchema.project b", q.toSql());
    }
    
    public void testToSql() throws Exception {
        SelectItem selectItem = new SelectItem(_schema.getTableByName(TABLE_PROJECT).getColumns().get(0));
        assertEquals("project.project_id", selectItem.toSql());
    }
    
    public void testToSqlFuntionApproximation() throws Exception {
        SelectItem selectItem = new SelectItem(FunctionType.MAX, _schema.getTableByName(TABLE_PROJECT).getColumns().get(0));
        selectItem.setFunctionApproximationAllowed(true);
        assertEquals("APPROXIMATE MAX(project.project_id)", selectItem.toSql());
    }

    public void testSubQuerySelectItem() throws Exception {
        Table projectTable = _schema.getTableByName(TABLE_PROJECT);
        Table roleTable = _schema.getTableByName(TABLE_ROLE);

        Column projectIdColumn = projectTable.getColumnByName(COLUMN_PROJECT_PROJECT_ID);

        FromItem leftSide = new FromItem(projectTable);
        leftSide.setAlias("a");
        SelectItem[] leftOn = new SelectItem[] { new SelectItem(projectIdColumn) };

        Query subQuery = new Query();
        FromItem subQueryFrom = new FromItem(roleTable);
        subQueryFrom.setAlias("c");
        subQuery.from(subQueryFrom);
        List<Column> columns = roleTable.getColumns();
        subQuery.select(columns);

        SelectItem subQuerySelectItem = subQuery.getSelectClause().getSelectItem(columns.get(1));
        FromItem rightSide = new FromItem(subQuery);
        rightSide.setAlias("b");
        SelectItem[] rightOn = new SelectItem[] { subQuerySelectItem };
        FromItem from = new FromItem(JoinType.LEFT, leftSide, rightSide, leftOn, rightOn);

        assertEquals(
                "MetaModelSchema.project a LEFT JOIN (SELECT c.contributor_id, c.project_id, c.name FROM MetaModelSchema.role c) b ON a.project_id = b.project_id",
                from.toString());

        Query q = new Query();
        q.from(from);
        try {
            new SelectItem(subQuerySelectItem, from);
            fail("Exception should have been thrown!");
        } catch (IllegalArgumentException e) {
            assertEquals("Only sub-query based FromItems allowed.", e.getMessage());
        }

        q.select(new SelectItem(subQuerySelectItem, rightSide));
        assertEquals(
                "SELECT b.project_id FROM MetaModelSchema.project a LEFT JOIN (SELECT c.contributor_id, c.project_id, c.name FROM MetaModelSchema.role c) b ON a.project_id = b.project_id",
                q.toString());
    }

    public void testGetSuperQueryAlias() throws Exception {
        SelectItem item = new SelectItem(FunctionType.COUNT, "*", "").setAlias(null);
        assertEquals("COUNT(*)", item.getSameQueryAlias());
        assertEquals("COUNT(*)", item.getSuperQueryAlias());
        
        item = new SelectItem(FunctionType.SUM, new MutableColumn("foo"));
        assertEquals("SUM(foo)", item.getSameQueryAlias());
        assertEquals("SUM(foo)", item.getSuperQueryAlias());
    }
}