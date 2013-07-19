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
package org.eobjects.metamodel.mongodb;

import java.util.List;

import org.eobjects.metamodel.data.AbstractDataSet;
import org.eobjects.metamodel.data.DefaultRow;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;

final class MongoDbDataSet extends AbstractDataSet {

	private static final Logger logger = LoggerFactory
			.getLogger(MongoDbDataSet.class);

	private final DBCursor _cursor;
	private final boolean _queryPostProcessed;

	private boolean _closed;
	private volatile DBObject _dbObject;

	public MongoDbDataSet(DBCursor cursor, Column[] columns,
			boolean queryPostProcessed) {
	    super(columns);
		_cursor = cursor;
		_queryPostProcessed = queryPostProcessed;
		_closed = false;
	}

	public boolean isQueryPostProcessed() {
		return _queryPostProcessed;
	}

	@Override
	public void close() {
		super.close();
		_cursor.close();
		_closed = true;
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if (!_closed) {
			logger.warn(
					"finalize() invoked, but DataSet is not closed. Invoking close() on {}",
					this);
			close();
		}
	}

	@Override
	public boolean next() {
		if (_cursor.hasNext()) {
			_dbObject = _cursor.next();
			return true;
		} else {
			_dbObject = null;
			return false;
		}
	}

	@Override
	public Row getRow() {
		if (_dbObject == null) {
			return null;
		}

		final int size = getHeader().size();
        final Object[] values = new Object[size];
		for (int i = 0; i < values.length; i++) {
			final SelectItem selectItem = getHeader().getSelectItem(i);
			final String key = selectItem.getColumn().getName();
			final Object value = _dbObject.get(key);
			values[i] = toValue(selectItem.getColumn(), value);
		}
		return new DefaultRow(getHeader(), values);
	}

	private Object toValue(Column column, Object value) {
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
