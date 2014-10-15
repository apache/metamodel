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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.metamodel.MetaModelException;

/**
 * Row publisher implementation used by {@link RowPublisherDataSet}.
 */
class RowPublisherImpl implements RowPublisher {

	public static final int BUFFER_SIZE = 20;

	private final RowPublisherDataSet _dataSet;
	private final BlockingQueue<Row> _queue;
	private final AtomicBoolean _finished;
	private final AtomicInteger _rowCount;
	private volatile Row _currentRow;
	private volatile Exception _error;

	public RowPublisherImpl(RowPublisherDataSet dataSet) {
		_dataSet = dataSet;
		_queue = new ArrayBlockingQueue<Row>(BUFFER_SIZE);
		_finished = new AtomicBoolean(false);
		_rowCount = new AtomicInteger();
	}

	@Override
	public boolean publish(Row row) {
		if (_finished.get()) {
			return false;
		}
		while (!offer(row)) {
			if (_finished.get()) {
				return false;
			}
			// wait one more cycle
		}
		int rowCount = _rowCount.incrementAndGet();
		if (_dataSet.getMaxRows() > 0 && rowCount >= _dataSet.getMaxRows()) {
			finished();
			return false;
		}
		return true;
	}

	private boolean offer(Row row) {
		try {
			return _queue.offer(row, 1000, TimeUnit.MICROSECONDS);
		} catch (InterruptedException e) {
			// do nothing
			return false;
		}
	}

	@Override
	public boolean publish(Object[] values) {
		Row row = new DefaultRow(_dataSet.getHeader(), values);
		return publish(row);
	}

	@Override
	public boolean publish(Object[] values, Style[] styles) {
		Row row = new DefaultRow(_dataSet.getHeader(), values, styles);
		return publish(row);
	}

	@Override
	public void finished() {
		_finished.set(true);
	}

	public boolean next() {
		if (_queue.isEmpty() && _finished.get()) {
			return false;
		}
		try {
			_currentRow = _queue.poll(1000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			// do nothing
		}
		if (_currentRow != null) {
			return true;
		}
		if (_error != null) {
			if (_error instanceof RuntimeException) {
				throw (RuntimeException) _error;
			}
			throw new MetaModelException(_error);
		}
		// "busy" (1 second) wait
		return next();
	}

	public Row getRow() {
		return _currentRow;
	}

	public void failed(Exception error) {
		_error = error;
	}
}
