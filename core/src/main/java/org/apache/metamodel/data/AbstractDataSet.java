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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.util.BaseObject;

/**
 * Abstract DataSet implementation. Provides convenient implementations of
 * trivial method and reusable parts of non-trivial methods of a DataSet.
 */
public abstract class AbstractDataSet extends BaseObject implements DataSet {

    private final DataSetHeader _header;

    public AbstractDataSet(SelectItem[] selectItems) {
        this(Arrays.asList(selectItems));
    }

    public AbstractDataSet(List<SelectItem> selectItems) {
        this(new CachingDataSetHeader(selectItems));
    }

    /**
     * Constructor appropriate for dataset implementations that wrap other
     * datasets, such as the {@link MaxRowsDataSet}, {@link FilteredDataSet} and
     * more.
     * 
     * @param dataSet
     */
    public AbstractDataSet(DataSet dataSet) {
        if (dataSet instanceof AbstractDataSet) {
            _header = ((AbstractDataSet) dataSet).getHeader();
        } else {
            _header = new CachingDataSetHeader(Arrays.asList(dataSet.getSelectItems()));
        }
    }

    public AbstractDataSet(DataSetHeader header) {
        _header = Objects.requireNonNull(header);
    }

    public AbstractDataSet(Column[] columns) {
        this(MetaModelHelper.createSelectItems(columns));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SelectItem[] getSelectItems() {
        return getHeader().getSelectItems();
    }

    protected DataSetHeader getHeader() {
        return _header;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int indexOf(SelectItem item) {
        return getHeader().indexOf(item);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final List<Object[]> toObjectArrays() {
        try {
            List<Object[]> objects = new ArrayList<Object[]>();
            while (next()) {
                Row row = getRow();
                objects.add(row.getValues());
            }
            return objects;
        } finally {
            close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "DataSet[selectItems=" + Arrays.toString(getSelectItems()) + "]";
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(getClass());
        identifiers.add(getSelectItems());
    }

    @Override
    public List<Row> toRows() {
        try {
            List<Row> result = new ArrayList<Row>();
            while (next()) {
                result.add(getRow());
            }
            return result;
        } finally {
            close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Row> iterator() {
        return new DataSetIterator(this);
    }
}