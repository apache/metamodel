package org.apache.metamodel.arff;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.util.NumberComparator;

import com.opencsv.CSVParser;
import com.opencsv.ICSVParser;

public class ArffDataSet extends AbstractDataSet {

    private final ICSVParser csvParser = new CSVParser(',', '\'');
    private final BufferedReader reader;
    private final int[] valueIndices;
    private final ColumnType[] valueTypes;

    private String line;

    public ArffDataSet(List<Column> columns, BufferedReader reader) {
        super(columns.stream().map(c -> new SelectItem(c)).collect(Collectors.toList()));
        this.valueIndices = columns.stream().mapToInt(Column::getColumnNumber).toArray();
        this.valueTypes = columns.stream().map(Column::getType).toArray(ColumnType[]::new);
        this.reader = reader;
    }

    @Override
    public boolean next() {
        try {
            line = reader.readLine();
            while (line != null && ArffDataContext.isIgnoreLine(line)) {
                line = reader.readLine();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return line != null;
    }

    @Override
    public Row getRow() {
        if (line == null) {
            return null;
        }
        final String[] stringValues;
        try {
            stringValues = csvParser.parseLine(line);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        final Object[] values = new Object[valueIndices.length];
        for (int i = 0; i < valueIndices.length; i++) {
            final int index = valueIndices[i];
            final String stringValue = stringValues[index];
            final ColumnType type = valueTypes[i];
            if (type.isNumber()) {
                if (stringValue.isEmpty() || "?".equals(stringValue)) {
                    values[i] = null;
                } else {
                    final Number n = NumberComparator.toNumber(stringValue);
                    if (type == ColumnType.INTEGER) {
                        values[i] = n.intValue();
                    } else {
                        values[i] = n;
                    }
                }
            } else if (type.isTimeBased()) {
                // TODO: extract format from column remarks
                try {
                    values[i] = new SimpleDateFormat("yyyy-MM-dd").parse(stringValue);
                } catch (ParseException e) {
                    throw new IllegalStateException(e);
                }
            } else {
                values[i] = stringValue;
            }
        }

        return new DefaultRow(getHeader(), values);
    }

    @Override
    public void close() {
        super.close();
    }
}
