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

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.metamodel.query.InvokableQuery;
import org.apache.metamodel.util.ImmutableRef;

/**
 * A {@link DataSet} that represents the union of two or more other data sets
 */
public class UnionDataSet extends AbstractDataSet implements WrappingDataSet {

    private final Iterable<Supplier<DataSet>> _dataSetProviders;
    private Iterator<Supplier<DataSet>> _iterator;
    private DataSet _currentDataSet;

    public static DataSet ofQueries(DataSetHeader header, Collection<InvokableQuery> queries) {
        final Function<InvokableQuery, Supplier<DataSet>> mapper = q -> {
            return () -> {
                return q.execute();
            };
        };
        return new UnionDataSet(header, queries.stream().map(mapper).collect(Collectors.toList()));
    }

    public static DataSet ofDataSets(DataSetHeader header, Collection<DataSet> dataSets) {
        return new UnionDataSet(header, dataSets.stream().map(ds -> ImmutableRef.of(ds)).collect(Collectors.toList()));
    }

    private UnionDataSet(DataSetHeader header, Iterable<Supplier<DataSet>> dataSetProviders) {
        super(header);
        _dataSetProviders = Objects.requireNonNull(dataSetProviders);
    }

    @Override
    public boolean next() {
        if (_iterator == null) {
            _iterator = _dataSetProviders.iterator();
        }

        while (_currentDataSet == null || !_currentDataSet.next()) {
            if (!_iterator.hasNext()) {
                _currentDataSet = null;
                return false;
            }
            _currentDataSet = _iterator.next().get();
            assert getHeader().size() == _currentDataSet.getSelectItems().length;
        }
        return true;
    }

    @Override
    public Row getRow() {
        if (_currentDataSet == null) {
            return null;
        }
        return _currentDataSet.getRow();
    }

    @Override
    public DataSet getWrappedDataSet() {
        return _currentDataSet;
    }
}
