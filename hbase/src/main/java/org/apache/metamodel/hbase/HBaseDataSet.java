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
package org.apache.metamodel.hbase;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HBaseDataSet extends AbstractDataSet {

    private static final Logger logger = LoggerFactory.getLogger(HBaseDataSet.class);

    private final ResultScanner _scanner;
    private final org.apache.hadoop.hbase.client.Table _hTable;
    private volatile Result _nextResult;

    public HBaseDataSet(List<Column> columns, ResultScanner scanner, org.apache.hadoop.hbase.client.Table hTable) {
        super(columns.stream().map(SelectItem::new).collect(Collectors.toList()));
        _scanner = scanner;
        _hTable = hTable;
    }

    @Override
    public void close() {
        super.close();
        try {
            _scanner.close();
        } catch (Exception e) {
            logger.warn("Failed to close ResultScanner", e);
        }
        try {
            _hTable.close();
        } catch (Exception e) {
            logger.warn("Failed to close HTable", e);
        }
    }

    @Override
    public boolean next() {
        try {
            _nextResult = _scanner.next();
        } catch (IOException e) {
            throw new MetaModelException(e);
        }
        return _nextResult != null;
    }

    @Override
    public Row getRow() {
        return new HBaseRow(getHeader(), _nextResult);
    }

}
