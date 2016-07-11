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
package org.apache.metamodel.cassandra;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A utility class for Cassandra module.
 */
public class CassandraUtils {

    /**
     * Converts a Cassandra Row data object {@link com.datastax.driver.core.Row}
     * into MetaModel {@link org.apache.metamodel.data.Row}.
     *
     * @param dbObject
     *            a Cassandra object storing data.
     * @param header
     *            a header describing the columns of the data stored.
     * @return the MetaModel {@link org.apache.metamodel.data.Row} result
     *         object.
     */
    public static Row toRow(com.datastax.driver.core.Row dbObject, DataSetHeader header) {
        if (dbObject == null) {
            return null;
        }

        final int size = header.size();

        final Object[] values = new Object[size];
        for (int i = 0; i < values.length; i++) {
            final SelectItem selectItem = header.getSelectItem(i);
            final String key = selectItem.getColumn().getName();
            values[i] = getColumnValue(key, dbObject);
        }
        return new DefaultRow(header, values);
    }

    private static Object getColumnValue(String columnName, com.datastax.driver.core.Row row) {
        ColumnDefinitions columns = row.getColumnDefinitions();
        DataType columnType = columns.getType(columnName);
        switch (columnType.getName()) {
        case BIGINT:
            return row.getVarint(columnName);
        case COUNTER:
            return row.getLong(columnName);
        case BLOB:
            return row.getBytes(columnName);
        case BOOLEAN:
            return row.getBool(columnName);
        case DECIMAL:
            return row.getDecimal(columnName);
        case DOUBLE:
            return row.getDouble(columnName);
        case FLOAT:
            return row.getFloat(columnName);
        case INT:
            return row.getInt(columnName);
        case TEXT:
            return row.getString(columnName);
        case TIMESTAMP:
            return row.getTimestamp(columnName);
        case UUID:
            return row.getUUID(columnName);
        case VARCHAR:
            return row.getString(columnName);
        case VARINT:
            return row.getVarint(columnName);
        case LIST:
            return row.getList(columnName, List.class);
        case MAP:
            return row.getMap(columnName, Map.class, String.class);
        case SET:
            return row.getSet(columnName, Set.class);
        case INET:
            return row.getInet(columnName);
        default:
            return row.getString(columnName);
        }
    }

}
