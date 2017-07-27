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

import java.util.Arrays;
import java.util.List;

import org.apache.metamodel.query.SelectItem;

/**
 * DataSet implementation based on in-memory data.
 */
public final class InMemoryDataSet extends AbstractDataSet {

    private final List<Row> _rows;
    private int _rowNumber = -1;

    public InMemoryDataSet(Row... rows) {
        this(Arrays.asList(rows));
    }

    public InMemoryDataSet(List<Row> rows) {
        this(getHeader(rows), rows);
    }

    public InMemoryDataSet(DataSetHeader header, Row... rows) {
        super(header);
        _rows = Arrays.asList(rows);
    }

    public InMemoryDataSet(DataSetHeader header, List<Row> rows) {
        super(header);
        _rows = rows;
    }

    private static DataSetHeader getHeader(List<Row> rows) {
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Cannot hold an empty list of rows, use " + EmptyDataSet.class
                    + " for this");
        }

        final List<SelectItem> selectItems = rows.get(0).getSelectItems();

        if (rows.size() > 3) {
            // not that many records - caching will not have body to scale
            return new SimpleDataSetHeader(selectItems);
        }
        return new CachingDataSetHeader(selectItems);
    }

    @Override
    public boolean next() {
        _rowNumber++;
        if (_rowNumber < _rows.size()) {
            return true;
        }
        return false;
    }

    @Override
    public Row getRow() {
        if (_rowNumber < 0 || _rowNumber >= _rows.size()) {
            return null;
        }
        Row row = _rows.get(_rowNumber);
        assert row.size() == getHeader().size();
        return row;
    }

    public List<Row> getRows() {
        return _rows;
    }

    public int size() {
        return _rows.size();
    }
}