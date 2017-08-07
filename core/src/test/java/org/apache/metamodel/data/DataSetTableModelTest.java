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
package org.apache.metamodel.data;

import java.util.ArrayList;
import java.util.List;

import javax.swing.table.TableModel;

import com.google.common.collect.Lists;
import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.MutableColumn;

import junit.framework.TestCase;

public class DataSetTableModelTest extends TestCase {

    public void testToTableModel() throws Exception {
        SelectItem[] selectItems = MetaModelHelper.createSelectItems(new MutableColumn("CUSTOMERNUMBER"),
                new MutableColumn("CUSTOMERNAME"), new MutableColumn("CONTACTLASTNAME"));
        CachingDataSetHeader header = new CachingDataSetHeader(Lists.newArrayList(selectItems));
        List<Row> rows = new ArrayList<Row>();
        rows.add(new DefaultRow(header, new Object[] { 1, "John", "Doe" }));
        rows.add(new DefaultRow(header, new Object[] { 2, "John", "Taylor" }));
        DataSet data = new InMemoryDataSet(header, rows);

        TableModel tableModel = new DataSetTableModel(data);
        data.close();

        assertEquals(3, tableModel.getColumnCount());
        assertEquals("CUSTOMERNUMBER", tableModel.getColumnName(0));
        assertEquals("CUSTOMERNAME", tableModel.getColumnName(1));
        assertEquals("CONTACTLASTNAME", tableModel.getColumnName(2));
        assertEquals(2, tableModel.getRowCount());

        // Take a small sample from the data
        assertEquals("Taylor", tableModel.getValueAt(1, 2).toString());
        
    }
}