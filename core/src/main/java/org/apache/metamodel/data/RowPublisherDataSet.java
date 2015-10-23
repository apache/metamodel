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

import java.io.Closeable;

import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.util.Action;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.SharedExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract {@link DataSet} implementation for use in scenarios where a
 * pull-oriented style of reading data is not supported. This implementation
 * instead allows a publshing action to publish rows to the dataset in a
 * blocking manner, and thereby to adapt without having to load all rows into
 * memory.
 */
public final class RowPublisherDataSet extends AbstractDataSet {

    private static final Logger logger = LoggerFactory.getLogger(RowPublisherDataSet.class);

    private final int _maxRows;
    private final Action<RowPublisher> _publishAction;
    private final Closeable[] _closeables;
    private RowPublisherImpl _rowPublisher;
    private boolean _closed;

    public RowPublisherDataSet(SelectItem[] selectItems, int maxRows, Action<RowPublisher> publishAction) {
        this(selectItems, maxRows, publishAction, new Closeable[0]);
    }

    public RowPublisherDataSet(SelectItem[] selectItems, int maxRows, Action<RowPublisher> publishAction,
            Closeable... closeables) {
        super(selectItems);
        _maxRows = maxRows;
        _publishAction = publishAction;
        _closed = false;
        _closeables = closeables;
    }

    public int getMaxRows() {
        return _maxRows;
    }

    @Override
    public void close() {
        super.close();
        _closed = true;
        if (_rowPublisher != null) {
            _rowPublisher.finished();
            _rowPublisher = null;
        }
        if (_closeables != null) {
            FileHelper.safeClose((Object[]) _closeables);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!_closed) {
            logger.warn("finalize() invoked, but DataSet is not closed. Invoking close() on {}", this);
            close();
        }
    }

    @Override
    public boolean next() {
        if (_rowPublisher == null) {
            // first time, create the publisher
            _rowPublisher = new RowPublisherImpl(this);
            logger.info("Starting separate thread for publishing action: {}", _publishAction);
            Runnable runnable = new Runnable() {
                public void run() {
                    boolean successful = false;
                    try {
                        _publishAction.run(_rowPublisher);
                        logger.debug("Publshing action finished!");
                        successful = true;
                    } catch (Exception e) {
                        _rowPublisher.failed(e);
                    }
                    if (successful) {
                        _rowPublisher.finished();
                    }
                };
            };
            SharedExecutorService.get().submit(runnable);
        }
        return _rowPublisher.next();
    }

    @Override
    public Row getRow() {
        if (_rowPublisher == null) {
            return null;
        }
        return _rowPublisher.getRow();
    }

}
