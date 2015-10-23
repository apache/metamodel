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

/**
 * Wraps another DataSet and enforces a first row offset.
 */
public final class FirstRowDataSet extends AbstractDataSet implements WrappingDataSet {

    private final DataSet _dataSet;
    private volatile int _rowsLeftToSkip;

    /**
     * Constructs a {@link FirstRowDataSet}.
     * 
     * @param dataSet
     *            the dataset to wrap
     * @param firstRow
     *            the first row number (1-based).
     */
    public FirstRowDataSet(DataSet dataSet, int firstRow) {
        super(dataSet);
        _dataSet = dataSet;
        if (firstRow < 1) {
            throw new IllegalArgumentException("First row cannot be negative or zero");
        }
        _rowsLeftToSkip = firstRow - 1;
    }

    @Override
    public void close() {
        _dataSet.close();
    }

    @Override
    public Row getRow() {
        return _dataSet.getRow();
    }
    
    @Override
    public DataSet getWrappedDataSet() {
        return _dataSet;
    }

    @Override
    public boolean next() {
        boolean next = true;
        if (_rowsLeftToSkip > 0) {
            while (_rowsLeftToSkip > 0) {
                next = _dataSet.next();
                if (next) {
                    _rowsLeftToSkip--;
                } else {
                    // no more rows at all - exit loop
                    _rowsLeftToSkip = 0;
                    return false;
                }
            }
        }
        return _dataSet.next();
    }
}