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

package org.eobjects.metamodel.data;

import java.util.ArrayList;
import java.util.List;

import javax.swing.table.TableModel;

import org.eobjects.metamodel.MetaModelHelper;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.MutableColumn;

import junit.framework.TestCase;

public class DataSetTableModelTest extends TestCase {

    public void testToTableModel() throws Exception {
        SelectItem[] selectItems = MetaModelHelper.createSelectItems(new MutableColumn("CUSTOMERNUMBER"),
                new MutableColumn("CUSTOMERNAME"), new MutableColumn("CONTACTLASTNAME"));
        CachingDataSetHeader header = new CachingDataSetHeader(selectItems);
        List<Row> rows = new ArrayList<Row>();
        rows.add(new DefaultRow(header, new Object[] { 1, "John", "Doe" }));
        rows.add(new DefaultRow(header, new Object[] { 2, "John", "Taylor" }));
        DataSet data = new InMemoryDataSet(header, rows);

        @SuppressWarnings("deprecation")
        TableModel tableModel = data.toTableModel();
        assertEquals(3, tableModel.getColumnCount());
        assertEquals("CUSTOMERNUMBER", tableModel.getColumnName(0));
        assertEquals("CUSTOMERNAME", tableModel.getColumnName(1));
        assertEquals("CONTACTLASTNAME", tableModel.getColumnName(2));
        assertEquals(2, tableModel.getRowCount());

        // Take a small sample from the data
        assertEquals("Taylor", tableModel.getValueAt(1, 2).toString());
    }
}