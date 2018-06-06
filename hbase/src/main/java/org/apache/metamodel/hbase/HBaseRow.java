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

import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.metamodel.data.AbstractRow;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.schema.Column;

/**
 * Row implementation around a HBase result
 */
final class HBaseRow extends AbstractRow implements Row {

    private static final long serialVersionUID = 1L;

    private final DataSetHeader _header;
    private final Result _result;

    public HBaseRow(DataSetHeader header, Result result) {
        _header = header;
        _result = result;
    }

    @Override
    protected DataSetHeader getHeader() {
        return _header;
    }

    @Override
    public Object getValue(int index) throws IndexOutOfBoundsException {
        final Column column = _header.getSelectItem(index).getColumn();
        final String name = column.getName();
        if (HBaseDataContext.FIELD_ID.equals(name)) {
            byte[] rowKey = _result.getRow();
            if (column.getType().isLiteral()) {
                return Bytes.toString(rowKey);
            }
            return rowKey;
        }

        final int colonIndex = name.indexOf(':');
        if (colonIndex != -1) {
            byte[] family = name.substring(0, colonIndex).getBytes();
            byte[] qualifier = name.substring(colonIndex + 1).getBytes();
            byte[] value = _result.getValue(family, qualifier);
            if (value == null) {
                return null;
            }
            if (column.getType().isLiteral()) {
                return Bytes.toString(value);
            }
            return value;
        } else {
            final NavigableMap<byte[], byte[]> map = _result.getFamilyMap(name.getBytes());
            if (map == null || map.isEmpty()) {
                return map;
            }
            return new HBaseFamilyMap(map);
        }
    }

    @Override
    public Style getStyle(int index) throws IndexOutOfBoundsException {
        return Style.NO_STYLE;
    }

}
