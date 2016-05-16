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
package org.apache.metamodel.mongodb.common;

import java.util.List;
import java.util.Map;

import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.util.CollectionUtils;

import com.mongodb.DBObject;

/**
 * A utility class for MongoDB module.
 */
public class MongoDBUtils {

    /**
     * Converts a MongoDB data object {@link DBObject} into MetaModel
     * {@link Row}.
     * 
     * @param dbObject
     *            a MongoDB object storing data.
     * @param header
     *            a header describing the columns of the data stored.
     * @return the MetaModel {@link Row} result object.
     */
    public static Row toRow(DBObject dbObject, DataSetHeader header) {
        if (dbObject == null) {
            return null;
        }

        final Map<?,?> map = dbObject.toMap();
        return toRow(map, header);
    }
    
    /**
     * Converts a map into MetaModel. This map stores data of a MongoDB document.
     * 
     * @param map
     *            a map object storing data of a MongoDB document.
     * @param header
     *            a header describing the columns of the data stored.
     * @return the MetaModel {@link Row} result object.
     */
    public static Row toRow(Map<?,?> map, DataSetHeader header) {
        if (map == null) {
            return null;
        }

        final int size = header.size();
        final Object[] values = new Object[size];
        for (int i = 0; i < values.length; i++) {
            final SelectItem selectItem = header.getSelectItem(i);
            final String key = selectItem.getColumn().getName();
            final Object value = CollectionUtils.find(map, key);
            values[i] = toValue(selectItem.getColumn(), value);
        }
        return new DefaultRow(header, values);
    }

    private static Object toValue(Column column, Object value) {
        if (value instanceof List) {
            return value;
        }
        if (value instanceof DBObject) {
            DBObject basicDBObject = (DBObject) value;
            return basicDBObject.toMap();
        }
        return value;
    }

}
