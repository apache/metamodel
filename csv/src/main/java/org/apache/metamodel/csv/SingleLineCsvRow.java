package org.apache.metamodel.csv;

import org.apache.metamodel.data.AbstractRow;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.util.LazyRef;

import au.com.bytecode.opencsv.CSVParser;

/**
 * Specialized row implementation for single-line CSV values
 */
final class SingleLineCsvRow extends AbstractRow {

    private static final long serialVersionUID = 1L;

    private final SingleLineCsvDataSet _dataSet;
    private final LazyRef<String[]> _valuesRef;

    public SingleLineCsvRow(SingleLineCsvDataSet dataSet, final String line, final int columnsInTable,
            final boolean failOnInconsistentRowLength, final int rowNumber) {
        _dataSet = dataSet;
        _valuesRef = new LazyRef<String[]>() {
            @Override
            protected String[] fetch() throws Throwable {
                final CSVParser parser = _dataSet.getCsvParser();
                final String[] values = parser.parseLine(line);

                if (failOnInconsistentRowLength) {
                    if (columnsInTable != values.length) {
                        throw new InconsistentRowLengthException(columnsInTable, SingleLineCsvRow.this, values,
                                rowNumber);
                    }
                }

                return values;
            }
        };
    }

    @Override
    public Object getValue(int index) throws IndexOutOfBoundsException {
        String[] values = _valuesRef.get();
        return values[index];
    }

    @Override
    public Style getStyle(int index) throws IndexOutOfBoundsException {
        return Style.NO_STYLE;
    }

    @Override
    protected DataSetHeader getHeader() {
        return _dataSet.getHeader();
    }

}
