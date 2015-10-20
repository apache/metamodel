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
package org.apache.metamodel.jdbc;

import java.util.List;

import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to calculate an appropriate fetch size of a JDBC query.
 * 
 * The approach used in this class is largely based on the documentation of
 * Oracle's caching size, see <a href=
 * "http://www.oracle.com/technetwork/database/enterprise-edition/memory.pdf"
 * >JDBC Memory Management</a>, section "Where does it all go?".
 */
final class FetchSizeCalculator {

	/**
	 * 22 bytes is a reasonable approximation for remaining row types, we add a
	 * few bytes to be on the safe side.
	 */
	private static final int DEFAULT_COLUMN_SIZE = 30;

	/**
	 * A kilobyte (kb)
	 */
	private static final int KB = 1024;

	private static final Logger logger = LoggerFactory
			.getLogger(FetchSizeCalculator.class);

	private static final int MIN_FETCH_SIZE = 1;
	private static final int MAX_FETCH_SIZE = 25000;
	private final int _bytesInMemory;

	public FetchSizeCalculator(int bytesInMemory) {
		_bytesInMemory = bytesInMemory;
	}

	/**
	 * Gets the fetch size of a query
	 * 
	 * @param query
	 *            the query to execute
	 * @return an integer representing how many rows to eagerly fetch for the
	 *         query
	 */
	public int getFetchSize(Query query) {
		if (isSingleRowQuery(query)) {
			return 1;
		}
		int bytesPerRow = getRowSize(query);
		int result = getFetchSize(bytesPerRow);
		final Integer maxRows = query.getMaxRows();
		if (maxRows != null && result > maxRows) {
			logger.debug("Result ({}) was below max rows ({}), adjusting.",
					result, maxRows);
			result = maxRows;
		}
		return result;
	}

	/**
	 * Gets whether a query is guaranteed to only yield a single row. Such
	 * queries are queries that only consist of aggregation functions and no
	 * group by clause.
	 * 
	 * @param query
	 * @return
	 */
	private boolean isSingleRowQuery(Query query) {
		if (!query.getGroupByClause().isEmpty()) {
			return false;
		}

		List<SelectItem> items = query.getSelectClause().getItems();
		for (SelectItem item : items) {
			if (item.getAggregateFunction() == null) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Gets the fetch size of a query based on the columns to query.
	 * 
	 * @param columns
	 *            the columns to query
	 * @return an integer representing how many rows to eagerly fetch for the
	 *         query
	 */
	public int getFetchSize(Column... columns) {
		int bytesPerRow = getRowSize(columns);
		return getFetchSize(bytesPerRow);
	}

	/**
	 * Gets the size of a row (in bytes).
	 * 
	 * @param query
	 *            the query that will yield the rows
	 * @return an integer representing the size of a row from the given query
	 *         (in bytes).
	 */
	protected int getRowSize(Query query) {
		List<SelectItem> items = query.getSelectClause().getItems();
		int bytesPerRow = 0;
		for (SelectItem selectItem : items) {
			bytesPerRow += getValueSize(selectItem);
		}
		return bytesPerRow;
	}

	/**
	 * Gets the size of a row (in bytes).
	 * 
	 * @param columns
	 *            the columns in the row
	 * @return an integer representing the size of a row with the given columns
	 *         (in bytes).
	 */
	protected int getRowSize(Column... columns) {
		int bytesPerRow = 0;
		for (Column column : columns) {
			bytesPerRow += getValueSize(column);
		}
		return bytesPerRow;
	}

	/**
	 * Gets the principal fetch size for a query where a row has the given size.
	 * 
	 * @param bytesPerRow
	 *            the size (in bytes) of a single row in the result set.
	 * @return an appropriate fetch size.
	 */
	protected int getFetchSize(int bytesPerRow) {
		if (bytesPerRow == 0) {
			// prevent divide by zero
			return MAX_FETCH_SIZE;
		}
		int result = _bytesInMemory / bytesPerRow;
		if (result < MIN_FETCH_SIZE) {
			logger.debug(
					"Result ({}) was below minimum fetch size ({}), adjusting.",
					result, MIN_FETCH_SIZE);
			result = MIN_FETCH_SIZE;
		} else if (result > MAX_FETCH_SIZE) {
			logger.debug(
					"Result ({}) was above maximum fetch size ({}), adjusting.",
					result, MAX_FETCH_SIZE);
			result = MAX_FETCH_SIZE;
		}
		return result;
	}

	/**
	 * Gets the size (in bytes) of a single {@link SelectItem}
	 */
	protected int getValueSize(SelectItem selectItem) {
		Column column = selectItem.getColumn();
		if (column == null) {
			return DEFAULT_COLUMN_SIZE;
		} else {
			return getValueSize(column);
		}
	}

	/**
	 * Gets the size (in bytes) of a single {@link Column}
	 */
	protected int getValueSize(Column column) {
		ColumnType type = column.getType();
		if (type == null) {
			return DEFAULT_COLUMN_SIZE;
		} else {
			Integer columnSize = column.getColumnSize();
			if (columnSize == null) {
				// if column size is missing, then use
				// size-indifferent approach
				return getSize(type);
			} else if (columnSize > 10000 && !type.isLargeObject()) {
				// if column size is unrealistically high, then use
				// size-indifferent approach
				return getSize(type);
			} else {
				return getSize(type, columnSize);
			}
		}
	}

	/**
	 * Gets the size (in bytes) of a column with a specific {@link ColumnType}
	 * and size
	 */
	private int getSize(ColumnType type, int columnSize) {
		final int baseSize;
		if (type.isBinary()) {
			baseSize = 1;
		} else if (type.isBoolean()) {
			baseSize = 1;
		} else if (type.isLiteral()) {
			baseSize = 2;
		} else if (type.isNumber()) {
			baseSize = 16;
		} else {
			baseSize = DEFAULT_COLUMN_SIZE;
		}

		int result = baseSize * columnSize;

		if (type.isLargeObject()) {
			// assign at least 4KB for LOBs.
			result = Math.max(result, 4 * KB);
		}

		return result;
	}

	/**
	 * Gets the (approximate) size (in bytes) of a column with a specific
	 * {@link ColumnType}.
	 */
	private int getSize(ColumnType type) {
		if (type.isBinary()) {
			return 4 * KB;
		} else if (type.isBoolean()) {
			return 2;
		} else if (type.isLargeObject()) {
			return 4 * KB;
		} else if (type.isLiteral()) {
			return KB;
		} else if (type.isNumber()) {
			return 16;
		} else {
			return DEFAULT_COLUMN_SIZE;
		}
	}
}