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
package org.apache.metamodel.excel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.delete.AbstractRowDeletionBuilder;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Table;

final class ExcelDeleteBuilder extends AbstractRowDeletionBuilder {

    private final ExcelUpdateCallback _updateCallback;

    public ExcelDeleteBuilder(ExcelUpdateCallback updateCallback, Table table) {
        super(table);
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() throws MetaModelException {
        // close the update callback will flush any changes
        _updateCallback.close();

        // read the workbook without streaming, since this will not wrap it in a
        // streaming workbook implementation (which do not support random
        // accessing rows).
        final Workbook workbook = _updateCallback.getWorkbook(false);

        final String tableName = getTable().getName();
        final SelectItem[] selectItems = MetaModelHelper.createSelectItems(getTable().getColumns());
        final DataSetHeader header = new SimpleDataSetHeader(selectItems);
        final Sheet sheet = workbook.getSheet(tableName);

        final Iterator<Row> rowIterator = ExcelUtils.getRowIterator(sheet, _updateCallback.getConfiguration(), true);
        final List<Row> rowsToDelete = new ArrayList<Row>();
        while (rowIterator.hasNext()) {
            final Row excelRow = rowIterator.next();
            final DefaultRow row = ExcelUtils.createRow(workbook, excelRow, header);

            final boolean deleteRow = deleteRow(row);
            if (deleteRow) {
                rowsToDelete.add(excelRow);
            }
        }

        // reverse the list to not mess up any row numbers
        Collections.reverse(rowsToDelete);

        for (Row row : rowsToDelete) {
            sheet.removeRow(row);
        }
    }
}
