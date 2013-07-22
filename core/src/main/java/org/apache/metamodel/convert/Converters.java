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
package org.apache.metamodel.convert;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.RowBuilder;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.intercept.InterceptableDataContext;
import org.apache.metamodel.intercept.Interceptors;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.SuperColumnType;
import org.apache.metamodel.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class consists of static methods for decorating {@link DataContext}s
 * with {@link TypeConverter}s, which allows for automatic conversion of values
 * on data read and write operations.
 */
public final class Converters {

    private static final Logger logger = LoggerFactory.getLogger(Converters.class);

    private Converters() {
        // prevent instantiation
    }

    /**
     * Adds a type converter to a specific column in the {@link DataContext}.
     * 
     * @param dataContext
     *            the DataContext to decorate
     * @param column
     *            the column which holds values to convert
     * @param converter
     *            the converter to use on the specified column
     * @return a decorated DataContext, which should be used for successive
     *         operations on the data.
     */
    public static UpdateableDataContext addTypeConverter(UpdateableDataContext dataContext, Column column,
            TypeConverter<?, ?> converter) {
        return addTypeConverterInternally(dataContext, column, converter);
    }

    /**
     * Adds a type converter to a specific column in the {@link DataContext}.
     * 
     * @param dataContext
     *            the DataContext to decorate
     * @param column
     *            the column which holds values to convert
     * @param converter
     *            the converter to use on the specified column
     * @return a decorated DataContext, which should be used for successive
     *         operations on the data.
     */
    public static DataContext addTypeConverter(DataContext dataContext, Column column, TypeConverter<?, ?> converter) {
        return addTypeConverterInternally(dataContext, column, converter);
    }

    /**
     * Adds a collection of type converters to specific columns in the
     * {@link DataContext}
     * 
     * @param dataContext
     *            the DataContext to decorate
     * @param converters
     *            a map containing columns and mapped type converters.
     * @return a decorated DataContext, which should be used for successive
     *         operations on the data.
     */
    public static UpdateableDataContext addTypeConverters(UpdateableDataContext dataContext,
            Map<Column, TypeConverter<?, ?>> converters) {
        return addTypeConvertersInternally(dataContext, converters);
    }

    /**
     * Adds a collection of type converters to specific columns in the
     * {@link DataContext}
     * 
     * @param dataContext
     *            the DataContext to decorate
     * @param converters
     *            a map containing columns and mapped type converters.
     * @return a decorated DataContext, which should be used for successive
     *         operations on the data.
     */
    public static DataContext addTypeConverters(DataContext dataContext, Map<Column, TypeConverter<?, ?>> converters) {
        return addTypeConvertersInternally(dataContext, converters);
    }

    /**
     * Auto-detects / guesses the type converters to be applied on set of
     * columns. This method will query the String columns in order to assert
     * which columns are likely candidates for conversion.
     * 
     * As such this method is not guaranteed to pick the correct converters,
     * since data can change over time or other conversions can be requested.
     * 
     * @param dataContext
     *            the DataContext that holds the data.
     * @param columns
     *            the columns to inspect to find type conversion candidates.
     * @param sampleSize
     *            the max amount of rows to query for doing auto-detection. Use
     *            {@link Integer#MAX_VALUE} if no constraint should be put on
     *            the number of records to sample.
     * @return a map of {@link Column}s and {@link TypeConverter}s which can be
     *         used (eg. with the {@link #addTypeConverters(DataContext, Map)}
     *         method) to decorate the DataContext with type converters.
     */
    public static Map<Column, TypeConverter<?, ?>> autoDetectConverters(DataContext dataContext, Column[] columns,
            int sampleSize) {
        columns = MetaModelHelper.getColumnsBySuperType(columns, SuperColumnType.LITERAL_TYPE);
        final Map<Column, TypeConverter<?, ?>> result = new HashMap<Column, TypeConverter<?, ?>>();
        Table[] tables = MetaModelHelper.getTables(columns);
        for (Table table : tables) {
            Column[] tableColumns = MetaModelHelper.getTableColumns(table, columns);
            autoDetectConvertersInternally(dataContext, table, tableColumns, sampleSize, result);
        }
        return result;
    }

    /**
     * Auto-detects / guesses the type converters to be applied on a table. This
     * method will query the String columns of a table in order to assert which
     * columns are likely candidates for conversion.
     * 
     * As such this method is not guaranteed to pick the correct converters,
     * since data can change over time or other conversions can be requested.
     * 
     * @param dataContext
     *            the DataContext that holds the data.
     * @param table
     *            the table to inspect to find type conversion candidates. This
     *            table will hold all columns of the result.
     * @param sampleSize
     *            the max amount of rows to query for doing auto-detection. Use
     *            {@link Integer#MAX_VALUE} if no constraint should be put on
     *            the number of records to sample.
     * @return a map of {@link Column}s and {@link TypeConverter}s which can be
     *         used (eg. with the {@link #addTypeConverters(DataContext, Map)}
     *         method) to decorate the DataContext with type converters.
     */
    public static Map<Column, TypeConverter<?, ?>> autoDetectConverters(DataContext dataContext, Table table,
            int sampleSize) {
        final Map<Column, TypeConverter<?, ?>> result = new HashMap<Column, TypeConverter<?, ?>>();
        Column[] columns = table.getColumnsOfSuperType(SuperColumnType.LITERAL_TYPE);
        autoDetectConvertersInternally(dataContext, table, columns, sampleSize, result);
        return result;
    }

    private static void autoDetectConvertersInternally(DataContext dataContext, Table table, Column[] columns,
            int sampleSize, Map<Column, TypeConverter<?, ?>> result) {
        if (columns == null || columns.length == 0) {
            return;
        }

        Map<Column, ColumnTypeDetector> detectors = new HashMap<Column, ColumnTypeDetector>();
        for (Column column : columns) {
            detectors.put(column, new ColumnTypeDetector());
        }

        Query query = dataContext.query().from(table).select(columns).toQuery();
        if (sampleSize > 0 && sampleSize != Integer.MAX_VALUE) {
            query.setMaxRows(sampleSize);
        }
        DataSet dataSet = dataContext.executeQuery(query);
        try {
            while (dataSet.next()) {
                Row row = dataSet.getRow();
                for (Column column : columns) {
                    String stringValue = (String) row.getValue(column);
                    ColumnTypeDetector detector = detectors.get(column);
                    detector.registerValue(stringValue);
                }
            }
        } finally {
            dataSet.close();
        }
        for (Column column : columns) {
            ColumnTypeDetector detector = detectors.get(column);
            TypeConverter<?, ?> converter = detector.createConverter();
            if (converter != null) {
                result.put(column, converter);
            }
        }
    }

    private static InterceptableDataContext addTypeConvertersInternally(final DataContext dc,
            Map<Column, TypeConverter<?, ?>> converters) {
        if (converters == null) {
            throw new IllegalArgumentException("Converters cannot be null");
        }

        InterceptableDataContext interceptable = Interceptors.intercept(dc);

        Set<Entry<Column, TypeConverter<?, ?>>> entries = converters.entrySet();
        for (Entry<Column, TypeConverter<?, ?>> entry : entries) {
            Column column = entry.getKey();
            TypeConverter<?, ?> converter = entry.getValue();
            interceptable = addTypeConverterInternally(interceptable, column, converter);
        }

        return interceptable;
    }

    private static InterceptableDataContext addTypeConverterInternally(final DataContext dc, Column column,
            TypeConverter<?, ?> converter) {
        if (column == null) {
            throw new IllegalArgumentException("Column cannot be null");
        }

        InterceptableDataContext interceptable = Interceptors.intercept(dc);
        DataContext delegate = interceptable.getDelegate();

        boolean interceptDataSets = true;

        if (delegate instanceof HasReadTypeConverters) {
            // some DataContexts implement the HasTypeConverters interface,
            // which is preferred when available
            HasReadTypeConverters hasTypeConverter = (HasReadTypeConverters) delegate;
            hasTypeConverter.addConverter(column, converter);

            interceptDataSets = false;
        }

        addTypeConverterInterceptors(interceptable, column, converter, interceptDataSets);
        return interceptable;
    }

    private static void addTypeConverterInterceptors(InterceptableDataContext interceptable, Column column,
            TypeConverter<?, ?> converter, boolean interceptDataSets) {
        // intercept datasets (reads)
        if (interceptDataSets) {
            ConvertedDataSetInterceptor interceptor = interceptable.getDataSetInterceptors().getInterceptorOfType(
                    ConvertedDataSetInterceptor.class);
            if (interceptor == null) {
                interceptor = new ConvertedDataSetInterceptor();
                interceptable.addDataSetInterceptor(interceptor);
            }
            interceptor.addConverter(column, converter);
        }

        // intercept inserts (writes)
        {
            ConvertedRowInsertionInterceptor interceptor = interceptable.getRowInsertionInterceptors()
                    .getInterceptorOfType(ConvertedRowInsertionInterceptor.class);
            if (interceptor == null) {
                interceptor = new ConvertedRowInsertionInterceptor();
                interceptable.addRowInsertionInterceptor(interceptor);
            }
            interceptor.addConverter(column, converter);
        }

        // convert updates
        {
            ConvertedRowUpdationInterceptor interceptor = interceptable.getRowUpdationInterceptors()
                    .getInterceptorOfType(ConvertedRowUpdationInterceptor.class);
            if (interceptor == null) {
                interceptor = new ConvertedRowUpdationInterceptor();
                interceptable.addRowUpdationInterceptor(interceptor);
            }
            interceptor.addConverter(column, converter);
        }

        // converting deletes (as well as where-items in updates) should not be
        // applied, because the DataSet interceptor is anyways only working on
        // the output. In that sense it adds symetry to NOT support conversion
        // in the where clause of UPDATEs and DELETEs.
    }

    /**
     * Converts values in a {@link RowBuilder}.
     * 
     * @param rowBuilder
     * @param converters
     * @return
     */
    protected static <RB extends RowBuilder<?>> RB convertRow(RB rowBuilder, Map<Column, TypeConverter<?, ?>> converters) {
        Table table = rowBuilder.getTable();
        Column[] columns = table.getColumns();
        Row row = rowBuilder.toRow();
        for (Column column : columns) {
            @SuppressWarnings("unchecked")
            TypeConverter<?, Object> converter = (TypeConverter<?, Object>) converters.get(column);
            if (converter != null) {
                final int indexInRow = row.indexOf(column);
                final Object value = row.getValue(indexInRow);
                final Object physicalValue = converter.toPhysicalValue(value);
                logger.debug("Converted virtual value {} to {}", value, physicalValue);
                if (value == null && physicalValue == null && !rowBuilder.isSet(column)) {
                    logger.debug("Omitting implicit null value for column: {}", column);
                } else {
                    final Style style = row.getStyle(indexInRow);
                    rowBuilder.value(column, physicalValue, style);
                }
            }
        }
        return rowBuilder;
    }
}
