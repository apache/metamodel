/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */

package org.eobjects.metamodel.jdbc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.data.AbstractDataSet;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectItem;

/**
 * DataSet for split queries. Queries will be executed as needed, not at once.
 * 
 * @see org.eobjects.metamodel.jdbc.QuerySplitter
 */
final class SplitQueriesDataSet extends AbstractDataSet {

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