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
package org.apache.metamodel.intercept;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateSummary;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.delete.RowDeletionBuilder;
import org.apache.metamodel.drop.TableDropBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.query.CompiledQuery;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.builder.InitFromBuilder;
import org.apache.metamodel.query.builder.InitFromBuilderImpl;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.RowUpdationBuilder;

public class InterceptableDataContext implements UpdateableDataContext {

    private final DataContext _delegate;
    private final InterceptorList<DataSet> _dataSetInterceptors;
    private final InterceptorList<Query> _queryInterceptors;
    private final InterceptorList<Schema> _schemaInterceptors;
    private final InterceptorList<RowInsertionBuilder> _rowInsertionInterceptors;
    private final InterceptorList<RowUpdationBuilder> _rowUpdationInterceptors;
    private final InterceptorList<RowDeletionBuilder> _rowDeletionInterceptors;
    private final InterceptorList<TableCreationBuilder> _tableCreationInterceptors;
    private final InterceptorList<TableDropBuilder> _tableDropInterceptors;

    protected InterceptableDataContext(DataContext delegate) {
        _delegate = delegate;
        _dataSetInterceptors = new InterceptorList<DataSet>();
        _queryInterceptors = new InterceptorList<Query>();
        _schemaInterceptors = new InterceptorList<Schema>();
        _rowInsertionInterceptors = new InterceptorList<RowInsertionBuilder>();
        _rowUpdationInterceptors = new InterceptorList<RowUpdationBuilder>();
        _rowDeletionInterceptors = new InterceptorList<RowDeletionBuilder>();
        _tableCreationInterceptors = new InterceptorList<TableCreationBuilder>();
        _tableDropInterceptors = new InterceptorList<TableDropBuilder>();
    }

    public InterceptableDataContext addTableCreationInterceptor(TableCreationInterceptor interceptor) {
        _tableCreationInterceptors.add(interceptor);
        return this;
    }

    public InterceptableDataContext removeTableCreationInterceptor(TableCreationInterceptor interceptor) {
        _tableCreationInterceptors.remove(interceptor);
        return this;
    }

    public InterceptableDataContext addTableDropInterceptor(TableDropInterceptor interceptor) {
        _tableDropInterceptors.add(interceptor);
        return this;
    }

    public InterceptableDataContext removeTableDropInterceptor(TableDropInterceptor interceptor) {
        _tableDropInterceptors.remove(interceptor);
        return this;
    }

    public InterceptableDataContext addRowInsertionInterceptor(RowInsertionInterceptor interceptor) {
        _rowInsertionInterceptors.add(interceptor);
        return this;
    }

    public InterceptableDataContext removeRowInsertionInterceptor(RowInsertionInterceptor interceptor) {
        _rowInsertionInterceptors.remove(interceptor);
        return this;
    }

    public InterceptableDataContext addRowUpdationInterceptor(RowUpdationInterceptor interceptor) {
        _rowUpdationInterceptors.add(interceptor);
        return this;
    }

    public InterceptableDataContext removeRowUpdationInterceptor(RowUpdationInterceptor interceptor) {
        _rowUpdationInterceptors.remove(interceptor);
        return this;
    }

    public InterceptableDataContext addRowDeletionInterceptor(RowDeletionInterceptor interceptor) {
        _rowDeletionInterceptors.add(interceptor);
        return this;
    }

    public InterceptableDataContext removeRowDeletionInterceptor(RowDeletionInterceptor interceptor) {
        _rowDeletionInterceptors.remove(interceptor);
        return this;
    }

    public InterceptableDataContext addQueryInterceptor(QueryInterceptor interceptor) {
        _queryInterceptors.add(interceptor);
        return this;
    }

    public InterceptableDataContext removeQueryInterceptor(QueryInterceptor interceptor) {
        _queryInterceptors.remove(interceptor);
        return this;
    }

    public InterceptableDataContext addSchemaInterceptor(SchemaInterceptor interceptor) {
        _schemaInterceptors.add(interceptor);
        return this;
    }

    public InterceptableDataContext removeSchemaInterceptor(SchemaInterceptor interceptor) {
        _schemaInterceptors.remove(interceptor);
        return this;
    }

    public InterceptableDataContext addDataSetInterceptor(DataSetInterceptor interceptor) {
        _dataSetInterceptors.add(interceptor);
        return this;
    }

    public InterceptableDataContext removeDataSetInterceptor(DataSetInterceptor interceptor) {
        _dataSetInterceptors.remove(interceptor);
        return this;
    }

    public InterceptorList<DataSet> getDataSetInterceptors() {
        return _dataSetInterceptors;
    }

    public InterceptorList<Query> getQueryInterceptors() {
        return _queryInterceptors;
    }

    public InterceptorList<RowInsertionBuilder> getRowInsertionInterceptors() {
        return _rowInsertionInterceptors;
    }

    public InterceptorList<RowUpdationBuilder> getRowUpdationInterceptors() {
        return _rowUpdationInterceptors;
    }

    public InterceptorList<RowDeletionBuilder> getRowDeletionInterceptors() {
        return _rowDeletionInterceptors;
    }

    public InterceptorList<Schema> getSchemaInterceptors() {
        return _schemaInterceptors;
    }

    public InterceptorList<TableCreationBuilder> getTableCreationInterceptors() {
        return _tableCreationInterceptors;
    }

    public DataContext getDelegate() {
        return _delegate;
    }

    @Override
    public DataSet executeQuery(Query query) throws MetaModelException {
        query = _queryInterceptors.interceptAll(query);
        DataSet dataSet = _delegate.executeQuery(query);
        dataSet = _dataSetInterceptors.interceptAll(dataSet);
        return dataSet;
    }

    @Override
    public UpdateableDataContext refreshSchemas() {
        _delegate.refreshSchemas();
        return this;
    }

    @Override
    public List<Schema> getSchemas() throws MetaModelException {
        return _delegate.getSchemas().stream()
                .map(schema -> {
                    if(_schemaInterceptors.isEmpty()){
                        return schema;
                    }else{
                        return _schemaInterceptors.interceptAll(schema);
                    }

                }).collect(Collectors.toList());
    }

    @Override
    public List<String> getSchemaNames() throws MetaModelException {
        if (_schemaInterceptors.isEmpty()) {
            return _delegate.getSchemaNames();
        }

        return getSchemas().stream()
                .map(schema -> schema.getName())
                .collect(Collectors.toList());
    }

    @Override
    public Schema getDefaultSchema() throws MetaModelException {
        Schema schema = _delegate.getDefaultSchema();
        schema = _schemaInterceptors.interceptAll(schema);
        return schema;
    }

    @Override
    public Schema getSchemaByName(String name) throws MetaModelException {
        Schema schema = _delegate.getSchemaByName(name);
        schema = _schemaInterceptors.interceptAll(schema);
        return schema;
    }

    @Override
    public InitFromBuilder query() {
        return new InitFromBuilderImpl(this);
    }

    @Override
    public Column getColumnByQualifiedLabel(String columnName) {
        return _delegate.getColumnByQualifiedLabel(columnName);
    }

    @Override
    public Table getTableByQualifiedLabel(String tableName) {
        return _delegate.getTableByQualifiedLabel(tableName);
    }

    @Override
    public UpdateSummary executeUpdate(UpdateScript update) {
        if (!(_delegate instanceof UpdateableDataContext)) {
            throw new UnsupportedOperationException("Delegate is not an UpdateableDataContext");
        }
        final UpdateableDataContext delegate = (UpdateableDataContext) _delegate;

        if (_tableCreationInterceptors.isEmpty() && _tableDropInterceptors.isEmpty()
                && _rowInsertionInterceptors.isEmpty() && _rowUpdationInterceptors.isEmpty()
                && _rowDeletionInterceptors.isEmpty()) {
            return delegate.executeUpdate(update);
        }

        final UpdateScript interceptableUpdateScript = new InterceptableUpdateScript(this, update,
                _tableCreationInterceptors, _tableDropInterceptors, _rowInsertionInterceptors,
                _rowUpdationInterceptors, _rowDeletionInterceptors);
        return delegate.executeUpdate(interceptableUpdateScript);
    }

    @Override
    public Query parseQuery(String queryString) throws MetaModelException {
        return _delegate.parseQuery(queryString);
    }

    @Override
    public DataSet executeQuery(String queryString) throws MetaModelException {
        final Query query = parseQuery(queryString);
        final DataSet dataSet = executeQuery(query);
        return dataSet;
    }

    @Override
    public CompiledQuery compileQuery(Query query) {
        return _delegate.compileQuery(query);
    }

    @Override
    public DataSet executeQuery(CompiledQuery compiledQuery, Object... values) {
        return _delegate.executeQuery(compiledQuery, values);
    }
}