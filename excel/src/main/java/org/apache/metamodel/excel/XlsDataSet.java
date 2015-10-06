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

import java.util.Iterator;

import org.apache.poi.ss.usermodel.Workbook;
import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.util.FileHelper;

/**
 * Stream {@link DataSet} implementation for Excel support.
 */
final class XlsDataSet extends AbstractDataSet {

    private final Iterator<org.apache.poi.ss.usermodel.Row> _rowIterator;
    private final Workbook _workbook;

    private volatile org.apache.poi.ss.usermodel.Row _row;
    private volatile boolean _closed;

    /**
     * Creates an XLS dataset
     * 
     * @param selectItems
     *            the selectitems representing the columns of the table
     * @param workbook
     * @param rowIterator
     */
    public XlsDataSet(SelectItem[] selectItems, Workbook workbook,
            Iterator<org.apache.poi.ss.usermodel.Row> rowIterator) {
        super(selectItems);
        _workbook = workbook;
        _rowIterator = rowIterator;
        _closed = false;
    }

    @Override
    public boolean next() {
        if (_rowIterator.hasNext()) {
            _row = _rowIterator.next();
            return true;
        } else {
            _row = null;
            close();
            return false;
        }
    }

    @Override
    public Row getRow() {
        if (_closed) {
            return null;
        }

        return ExcelUtils.createRow(_workbook, _row, getHeader());
    }

    @Override
    public void close() {
        super.close();
        if (!_closed) {
            FileHelper.safeClose(_workbook);
            _closed = true;
        }
    }
}
