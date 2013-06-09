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

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.eobjects.metamodel.create.AbstractTableCreationBuilder;
import org.eobjects.metamodel.create.TableCreationBuilder;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

/**
 * {@link TableCreationBuilder} implementation for Excel spreadsheets.
 * 
 * @author Kasper Sørensen
 */
final class ExcelTableCreationBuilder extends AbstractTableCreationBuilder<ExcelUpdateCallback> {

    public ExcelTableCreationBuilder(ExcelUpdateCallback updateCallback, Schema schema, String name) {
        super(updateCallback, schema, name);
    }

    @Override
    public Table execute() {
        final ExcelUpdateCallback updateCallback = getUpdateCallback();
        final MutableTable table = getTable();

        final Sheet sheet = updateCallback.createSheet(table.getName());

        final int lineNumber = updateCallback.getConfiguration().getColumnNameLineNumber();
        if (lineNumber != ExcelConfiguration.NO_COLUMN_NAME_LINE) {
            final int zeroBasedLineNumber = lineNumber - 1;
            final Row row = sheet.createRow(zeroBasedLineNumber);
            final Column[] columns = table.getColumns();
            for (int i = 0; i < columns.length; i++) {
                final Column column = columns[i];
                final int columnNumber = column.getColumnNumber();
                row.createCell(columnNumber).setCellValue(column.getName());
            }
        }

        final MutableSchema schema = (MutableSchema) table.getSchema();
        schema.addTable((MutableTable) table);
        return table;
    }
}
