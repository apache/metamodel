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
package org.apache.metamodel.elasticsearch.rest;

import java.util.List;

import org.apache.metamodel.AbstractUpdateCallback;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.delete.RowDeletionBuilder;
import org.apache.metamodel.drop.TableDropBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.searchbox.action.Action;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.Bulk.Builder;
import io.searchbox.core.BulkResult;
import io.searchbox.core.BulkResult.BulkResultItem;
import io.searchbox.indices.Refresh;

/**
 * {@link UpdateCallback} implementation for
 * {@link ElasticSearchRestDataContext}.
 */
final class JestElasticSearchUpdateCallback extends AbstractUpdateCallback {

    private static final Logger logger = LoggerFactory.getLogger(JestElasticSearchUpdateCallback.class);

    private static final int BULK_BUFFER_SIZE = 1000;

    private Bulk.Builder bulkBuilder;
    private int bulkActionCount = 0;
    private final boolean isBatch;

    public JestElasticSearchUpdateCallback(ElasticSearchRestDataContext dataContext, boolean isBatch) {
        super(dataContext);
        this.isBatch = isBatch;
    }

    private boolean isBatch() {
        return isBatch;
    }

    @Override
    public ElasticSearchRestDataContext getDataContext() {
        return (ElasticSearchRestDataContext) super.getDataContext();
    }

    @Override
    public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
            IllegalStateException {
        return new JestElasticSearchCreateTableBuilder(this, schema, name);
    }

    @Override
    public boolean isDropTableSupported() {
        return true;
    }

    @Override
    public TableDropBuilder dropTable(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new JestElasticSearchDropTableBuilder(this, table);
    }

    @Override
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new JestElasticSearchInsertBuilder(this, table);
    }

    @Override
    public boolean isDeleteSupported() {
        return true;
    }

    @Override
    public RowDeletionBuilder deleteFrom(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new JestElasticSearchDeleteBuilder(this, table);
    }

    public void onExecuteUpdateFinished() {
        if (isBatch()) {
            flushBulkActions();
        }

        final String indexName = getDataContext().getIndexName();
        final Refresh refresh = new Refresh.Builder().addIndex(indexName).build();

        JestClientExecutor.execute(getDataContext().getElasticSearchClient(), refresh, false);
    }

    private void flushBulkActions() {
        if (bulkBuilder == null || bulkActionCount == 0) {
            // nothing to flush
            return;
        }
        final Bulk bulk = getBulkBuilder().build();
        logger.info("Flushing {} actions to ElasticSearch index {}", bulkActionCount, getDataContext().getIndexName());
        executeBlocking(bulk);

        bulkActionCount = 0;
        bulkBuilder = null;
    }

    public void execute(Action<?> action) {
        if (isBatch() && action instanceof BulkableAction) {
            final Bulk.Builder bulkBuilder = getBulkBuilder();
            bulkBuilder.addAction((BulkableAction<?>) action);
            bulkActionCount++;
            if (bulkActionCount == BULK_BUFFER_SIZE) {
                flushBulkActions();
            }
        } else {
            executeBlocking(action);
        }
    }

    private void executeBlocking(Action<?> action) {
        final JestResult result = JestClientExecutor.execute(getDataContext().getElasticSearchClient(), action);
        if (!result.isSucceeded()) {
            if (result instanceof BulkResult) {
                final List<BulkResultItem> failedItems = ((BulkResult) result).getFailedItems();
                for (int i = 0; i < failedItems.size(); i++) {
                    final BulkResultItem failedItem = failedItems.get(i);
                    logger.error("Bulk failed with item no. {} of {}: id={} op={} status={} error={}", i+1, failedItems.size(), failedItem.id, failedItem.operation, failedItem.status, failedItem.error);
                }
            }
            throw new MetaModelException(result.getResponseCode() + " - " + result.getErrorMessage());
        }
    }

    private Builder getBulkBuilder() {
        if (bulkBuilder == null) {
            bulkBuilder = new Bulk.Builder();
            bulkBuilder.defaultIndex(getDataContext().getIndexName());
        }
        return bulkBuilder;
    }
}
