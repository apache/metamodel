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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.WrappingDataSet;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;

/**
 * DataSet for split queries. Queries will be executed as needed, not at once.
 * 
 * @see org.apache.metamodel.jdbc.QuerySplitter
 */
final class SplitQueriesDataSet extends AbstractDataSet implements WrappingDataSet {

    private static final Logger logger = LoggerFactory.getLogger(SplitQueriesDataSet.class);
    private final DataContext _dataContext;
    private Iterator<Query> _queryIterator;
    private DataSet _currentDataSet;
    private int _queryIndex = 0;

    public SplitQueriesDataSet(DataContext dataContext, List<Query> splitQueries) {
        super(getSelectItems(splitQueries));
        if (dataContext == null || splitQueries == null) {
            throw new IllegalArgumentException("Arguments cannot be null");
        }
        _dataContext = dataContext;
        _queryIterator = splitQueries.iterator();
    }

    private static List<SelectItem> getSelectItems(List<Query> splitQueries) {
        if (splitQueries.isEmpty()) {
            return new ArrayList<SelectItem>(0);
        }
        return splitQueries.get(0).getSelectClause().getItems();
    }

    @Override
    public DataSet getWrappedDataSet() {
        return _currentDataSet;
    }

    @Override
    public void close() {
        if (_currentDataSet != null) {
            logger.debug("currentDataSet.close()");
            _currentDataSet.close();
        }
        _currentDataSet = null;
        _queryIterator = null;
    }

    @Override
    public Row getRow() throws MetaModelException {
        if (_currentDataSet != null) {
            return _currentDataSet.getRow();
        }
        throw new IllegalStateException("No rows available. Either DataSet is closed or next() hasn't been called");
    }

    @Override
    public boolean next() {
        boolean result;
        if (_currentDataSet == null) {
            result = false;
        } else {
            result = _currentDataSet.next();
        }
        if (!result && _queryIterator.hasNext()) {
            if (_currentDataSet != null) {
                logger.debug("currentDataSet.close()");
                _currentDataSet.close();
            }
            Query q = _queryIterator.next();
            _currentDataSet = _dataContext.executeQuery(q);
            if (logger.isDebugEnabled()) {
                _queryIndex++;
                logger.debug("Executing query #{}", _queryIndex);
            }
            result = next();
        }
        return result;
    }
}