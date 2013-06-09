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
package org.eobjects.metamodel.excel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.MetaModelHelper;
import org.eobjects.metamodel.data.DataSetHeader;
import org.eobjects.metamodel.data.DefaultRow;
import org.eobjects.metamodel.data.SimpleDataSetHeader;
import org.eobjects.metamodel.delete.AbstractRowDeletionBuilder;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Table;

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
