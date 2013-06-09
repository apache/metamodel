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
package org.eobjects.metamodel.query.builder;

import junit.framework.TestCase;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.MockDataContext;
import org.eobjects.metamodel.query.FunctionType;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Table;

public class SyntaxExamplesTest extends TestCase {

    private DataContext dc;
    private Table table1;
    private Table table2;
    private Column col1;
    private Column col2;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        dc = new MockDataContext("sch", "tab1", "foo");
        MutableSchema schema = (MutableSchema) dc.getDefaultSchema();
        table1 = schema.getTables()[0];
        schema.addTable(new MutableTable("tab2").setSchema(schema));
        table2 = schema.getTableByName("tab2");
        col1 = table1.getColumns()[0];
        col2 = table1.getColumns()[1];
    }

    public void testSchema() throws Exception {
        assertEquals("tab1", table1.getName());
        assertEquals("sch.tab1", table1.getQualifiedLabel());
    }

    public void testFromAlias() throws Exception {
        dc.query().from(table1).as("t");
    }

    public void testFromJoin() throws Exception {
        dc.query().from(table1).innerJoin(table2).on(col1, col2).select(col1);
    }

    public void testWhereOr() throws Exception {
        dc.query().from(table1).as("t").select(col2).where(col1).isNotNull().or(col1).isNull().orderBy(col1).asc();
    }

    public void testGroupBy() throws Exception {
        dc.query().from(table1).selectCount().select(col1).groupBy(col1).having(FunctionType.SUM, col1).greaterThan(3)
                .orderBy(col1).asc();
    }

    public void testMultipleTables() throws Exception {
        Query q = dc.query().from(table1).as("t1").and(table2).as("t2").select(col1).where(col1).greaterThan(col2)
                .orderBy(col2).desc().toQuery();
        assertEquals("SELECT t1.foo FROM sch.tab1 t1, sch.tab2 t2 " + "WHERE t1.foo > t1.bar ORDER BY t1.bar DESC",
                q.toSql());
    }
}