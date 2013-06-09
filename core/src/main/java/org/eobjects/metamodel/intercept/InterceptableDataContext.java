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
package org.eobjects.metamodel.intercept;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.UpdateableDataContext;
import org.eobjects.metamodel.create.TableCreationBuilder;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.delete.RowDeletionBuilder;
import org.eobjects.metamodel.drop.TableDropBuilder;
import org.eobjects.metamodel.insert.RowInsertionBuilder;
import org.eobjects.metamodel.query.CompiledQuery;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.builder.InitFromBuilder;
import org.eobjects.metamodel.query.builder.InitFromBuilderImpl;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.update.RowUpdationBuilder;
import org.eobjects.metamodel.util.HasNameMapper;

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
    public Schema[] getSchemas() throws MetaModelException {
        Schema[] schemas = _delegate.getSchemas();
        if (!_schemaInterceptors.isEmpty()) {
            for (int i = 0; i < schemas.length; i++) {
                schemas[i] = _schemaInterceptors.interceptAll(schemas[i]);
            }
        }
        return schemas;
    }

    @Override
    public String[] getSchemaNames() throws MetaModelException {
        if (_schemaInterceptors.isEmpty()) {
            return _delegate.getSchemaNames();
        }
        Schema[] schemas = getSchemas();
        String[] schemaNames = new String[schemas.length];
        for (int i = 0; i < schemaNames.length; i++) {
            schemaNames[i] = new HasNameMapper().eval(schemas[i]);
        }
        return schemaNames;
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
    public void executeUpdate(UpdateScript update) {
        if (!(_delegate instanceof UpdateableDataContext)) {
            throw new UnsupportedOperationException("Delegate is not an UpdateableDataContext");
        }
        final UpdateableDataContext delegate = (UpdateableDataContext) _delegate;

        if (_tableCreationInterceptors.isEmpty() && _tableDropInterceptors.isEmpty()
                && _rowInsertionInterceptors.isEmpty() && _rowUpdationInterceptors.isEmpty()
                && _rowDeletionInterceptors.isEmpty()) {
            delegate.executeUpdate(update);
            return;
        }

        UpdateScript interceptableUpdateScript = new InterceptableUpdateScript(this, update,
                _tableCreationInterceptors, _tableDropInterceptors, _rowInsertionInterceptors,
                _rowUpdationInterceptors, _rowDeletionInterceptors);
        delegate.executeUpdate(interceptableUpdateScript);
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