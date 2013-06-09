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

package org.eobjects.metamodel.dbase;

import java.io.Closeable;
import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.MetaModelHelper;
import org.eobjects.metamodel.QueryPostprocessDataContext;
import org.eobjects.metamodel.data.CachingDataSetHeader;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.DataSetHeader;
import org.eobjects.metamodel.data.DefaultRow;
import org.eobjects.metamodel.data.InMemoryDataSet;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.schema.TableType;
import org.eobjects.metamodel.util.FileHelper;
import org.xBaseJ.DBF;
import org.xBaseJ.fields.CharField;
import org.xBaseJ.fields.DateField;
import org.xBaseJ.fields.Field;
import org.xBaseJ.fields.FloatField;
import org.xBaseJ.fields.LogicalField;
import org.xBaseJ.fields.MemoField;
import org.xBaseJ.fields.NumField;
import org.xBaseJ.fields.PictureField;

/**
 * DataContext implementation for dBase database files.
 * 
 * @author Kasper Sørensen
 */
public final class DbaseDataContext extends QueryPostprocessDataContext implements Closeable {

    private final String filename;
    private DBF dbf;

    public DbaseDataContext(String filename) {
        this.filename = filename;
    }

    public DbaseDataContext(File file) {
        this.filename = file.getAbsolutePath();
    }

    private DBF getDbf() {
        if (dbf == null) {
            synchronized (this) {
                if (dbf == null) {
                    try {
                        dbf = new DBF(filename);
                    } catch (Exception e) {
                        throw new MetaModelException("Could not open DBF file");
                    }
                }
            }
        }
        return dbf;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        DBF dbf = getDbf();

        String schemaName = dbf.getName();
        int separatorIndex = Math.max(schemaName.lastIndexOf('/'), schemaName.lastIndexOf('\\'));
        if (separatorIndex != -1) {
            schemaName = schemaName.substring(separatorIndex + 1);
        }

        MutableSchema schema = new MutableSchema(schemaName);
        MutableTable table = new MutableTable(schemaName.substring(0, schemaName.length() - 4), TableType.TABLE, schema);
        schema.addTable(table);

        for (int i = 0; i < dbf.getFieldCount(); i++) {
            try {
                Field field = dbf.getField(i + 1);

                MutableColumn column = new MutableColumn(field.getName());
                ColumnType columnType = ColumnType.VARCHAR;

                if (field instanceof FloatField) {
                    columnType = ColumnType.FLOAT;
                } else if (field instanceof NumField) {
                    columnType = ColumnType.DOUBLE;
                } else if (field instanceof CharField) {
                    columnType = ColumnType.CHAR;
                } else if (field instanceof DateField) {
                    columnType = ColumnType.DATE;
                } else if (field instanceof MemoField) {
                    columnType = ColumnType.VARCHAR;
                } else if (field instanceof LogicalField) {
                    columnType = ColumnType.OTHER;
                } else if (field instanceof PictureField) {
                    columnType = ColumnType.OTHER;
                }

                column.setType(columnType);
                column.setTable(table);
                column.setColumnNumber(i);
                column.setNativeType("" + field.getType());
                column.setColumnSize(field.getLength());
                table.addColumn(column);
            } catch (Exception e) {
                throw new MetaModelException("Could not retrieve DBF field", e);
            }
        }

        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return getMainSchema().getName();
    }

    @Override
    public DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        DBF dbf = getDbf();
        synchronized (dbf) {
            int rowNum = 0;
            try {
                dbf.gotoRecord(1);
            } catch (Exception e) {
                throw new MetaModelException(e);
            }

            final SelectItem[] selectItems = MetaModelHelper.createSelectItems(columns);
            final DataSetHeader header = new CachingDataSetHeader(selectItems);

            final List<Row> rowValues = new LinkedList<Row>();
            while (maxRows < 0 || rowNum < maxRows) {
                rowNum++;
                try {
                    Object[] values = new Object[columns.length];
                    for (int i = 0; i < columns.length; i++) {
                        int fieldNumber = 1 + columns[i].getColumnNumber();
                        Field field = dbf.getField(fieldNumber);
                        values[i] = convert(field.get(), columns[i].getType());
                    }
                    rowValues.add(new DefaultRow(header, values));
                } catch (Exception e) {
                    throw new MetaModelException(e);
                }

                try {
                    dbf.read();
                } catch (Exception e) {
                    // this exception is thrown if all records have been read
                    if ("End Of File".equals(e.getMessage())) {
                        break;
                    }
                    throw new MetaModelException(e);
                }
            }
            return new InMemoryDataSet(header, rowValues);
        }
    }

    private Object convert(String stringValue, ColumnType type) {
        if (stringValue == null || stringValue.length() == 0) {
            return null;
        }
        switch (type) {
        case FLOAT:
            return Float.parseFloat(stringValue);
        case DOUBLE:
            return Double.parseDouble(stringValue);
        case DATE:
            try {
                Date date = new SimpleDateFormat("yyyyMMdd").parse(stringValue);
                return date;
            } catch (ParseException e) {
                throw new IllegalArgumentException(stringValue);
            }
        default:
            return stringValue;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }

    @Override
    public void close() {
        FileHelper.safeClose(dbf);
    }
}