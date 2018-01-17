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

import java.io.IOException;

import org.apache.metamodel.AbstractUpdateCallback;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.delete.RowDeletionBuilder;
import org.apache.metamodel.drop.TableDropBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link UpdateCallback} implementation for
 * {@link ElasticSearchRestDataContext}.
 */
final class ElasticSearchRestUpdateCallback extends AbstractUpdateCallback {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestUpdateCallback.class);

    private static final int BULK_BUFFER_SIZE = 1000;

    private BulkRequest bulkRequest;
    private int bulkActionCount = 0;
    private final boolean isBatch;

    public ElasticSearchRestUpdateCallback(ElasticSearchRestDataContext dataContext, boolean isBatch) {
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
        // return new JestElasticSearchCreateTableBuilder(this, schema, name);
        return null;
    }

    @Override
    public boolean isDropTableSupported() {
        return false;
    }

    @Override
    public TableDropBuilder dropTable(Table table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new ElasticSearchRestInsertBuilder(this, table);
    }

    @Override
    public boolean isDeleteSupported() {
        return true;
    }

    @Override
    public RowDeletionBuilder deleteFrom(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new ElasticSearchRestDeleteBuilder(this, table);
    }

    public void onExecuteUpdateFinished() {
        if (isBatch()) {
//            flushBulkActions();
        }
    }

//    private void flushBulkActions() {
//        if (bulkRequest == null || bulkActionCount == 0) {
//            // nothing to flush
//            return;
//        }
//        final Bulk bulk = getBulkRequest().build();
//        logger.info("Flushing {} actions to ElasticSearch index {}", bulkActionCount, getDataContext().getIndexName());
//        executeBlocking(bulk);
//
//        bulkActionCount = 0;
//        bulkRequest = null;
//    }

    public void execute(ActionRequest action) {
//        if (isBatch() && action instanceof BulkAction) {
//            final BulkRequest bulkRequest = getBulkRequest();
//            bulkRequest.addAction((BulkableAction<?>) action);
//            bulkActionCount++;
//            if (bulkActionCount == BULK_BUFFER_SIZE) {
//                flushBulkActions();
//            }
//        } else {
            executeBlocking(action);
//        }
    }

    private void executeBlocking(ActionRequest action) {
        ActionResponse result;
        try {
            result = getDataContext().getElasticSearchClient().execute(action);
        } catch (IOException e) {
            logger.warn("Could not execute command {} ", action, e);
            throw new MetaModelException("Could not execute " + action, e);
        }

        if (result instanceof BulkResponse && ((BulkResponse) result).hasFailures()) {
            if (result instanceof BulkResponse) {
                BulkItemResponse[] failedItems = ((BulkResponse) result).getItems();
                for (int i = 0; i < failedItems.length; i++) {

                    if (failedItems[i].isFailed()) {
                        final BulkItemResponse failedItem = failedItems[i];
                        logger.error("Bulk failed with item no. {} of {}: id={} op={} status={} error={}", i + 1,
                                failedItems.length, failedItem.getId(), failedItem.getOpType(), failedItem.status(),
                                failedItem.getFailureMessage());
                    }
                }
            }
        }
    }

    private BulkRequest getBulkRequest() {
        if (bulkRequest == null) {
            bulkRequest = new BulkRequest();
            //bulkRequest.defaultIndex(getDataContext().getIndexName());
        }
        return bulkRequest;
    }
}
