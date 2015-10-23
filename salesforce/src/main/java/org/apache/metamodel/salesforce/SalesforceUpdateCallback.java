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
package org.apache.metamodel.salesforce;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.metamodel.AbstractUpdateCallback;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.delete.RowDeletionBuilder;
import org.apache.metamodel.drop.TableDropBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.LogicalOperator;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.RowUpdationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;

/**
 * Update callback implementation for Salesforce.com datacontexts.
 */
final class SalesforceUpdateCallback extends AbstractUpdateCallback implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(SalesforceUpdateCallback.class);

    private static final int INSERT_BATCH_SIZE = 100;

    private final PartnerConnection _connection;
    private final List<SObject> _pendingInserts;

    public SalesforceUpdateCallback(SalesforceDataContext dataContext, PartnerConnection connection) {
        super(dataContext);
        _connection = connection;
        _pendingInserts = new ArrayList<SObject>();
    }

    @Override
    public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
            IllegalStateException {
        throw new UnsupportedOperationException("Table creation not supported for Salesforce.com.");
    }

    @Override
    public boolean isDropTableSupported() {
        return false;
    }

    @Override
    public boolean isCreateTableSupported() {
        return false;
    }

    @Override
    public TableDropBuilder dropTable(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        throw new UnsupportedOperationException("Table dropping not supported for Salesforce.com.");
    }

    @Override
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new SalesforceInsertBuilder(this, table);
    }

    @Override
    public boolean isDeleteSupported() {
        return true;
    }

    @Override
    public RowDeletionBuilder deleteFrom(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new SalesforceDeleteBuilder(this, table);
    }

    protected void delete(String[] ids) {
        flushInserts();
        try {
            _connection.delete(ids);
        } catch (ConnectionException e) {
            throw SalesforceUtils.wrapException(e, "Failed to delete objects in Salesforce");
        }
    }

    private void flushInserts() {
        if (_pendingInserts.isEmpty()) {
            return;
        }
        final SObject[] objectsToInsert = _pendingInserts.toArray(new SObject[_pendingInserts.size()]);
        _pendingInserts.clear();
        try {
            final SaveResult[] saveResults = _connection.create(objectsToInsert);
            checkSaveResults(saveResults, "insert");
        } catch (ConnectionException e) {
            throw SalesforceUtils.wrapException(e, "Failed to insert objects in Salesforce");
        }
    }

    private void checkSaveResults(SaveResult[] saveResults, String action) {
        int successes = 0;
        int errors = 0;
        com.sforce.soap.partner.Error firstError = null;

        for (final SaveResult saveResult : saveResults) {
            boolean success = saveResult.getSuccess();
            if (success) {
                logger.debug("Succesfully {}ed record with id={}", action, saveResult.getId());
                successes++;
            } else {
                final com.sforce.soap.partner.Error[] errorArray = saveResult.getErrors();

                if (!"insert".equals(action)) {
                    boolean onlyMalformedId = true;
                    for (com.sforce.soap.partner.Error error : errorArray) {
                        if (com.sforce.soap.partner.StatusCode.MALFORMED_ID == error.getStatusCode()) {
                            logger.debug("Encountered MALFORMED_ID error for {} action. Ignoring.", action);
                        } else {
                            onlyMalformedId = false;
                            break;
                        }
                    }
                    
                    if (onlyMalformedId) {
                        return;
                    }
                }
                
                errors++;

                for (com.sforce.soap.partner.Error error : errorArray) {
                    if (firstError == null) {
                        firstError = error;
                    }
                    if (logger.isErrorEnabled()) {
                        logger.error("Error reported by Salesforce for {} operation: {} - {} - {}", action,
                                error.getStatusCode(), error.getMessage(), Arrays.toString(error.getFields()));
                    }
                }
            }

            if (errors > 0) {
                throw new IllegalStateException(errors + " out of " + (errors + successes) + " object(s) could not be "
                        + action + "ed in Salesforce! The first error message was: '" + firstError.getMessage() + "' ("
                        + firstError.getStatusCode() + "). see error log for further details.");
            }
        }
    }

    @Override
    public boolean isUpdateSupported() {
        return true;
    }

    @Override
    public RowUpdationBuilder update(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new SalesforceUpdateBuilder(this, table);
    }

    protected void insert(SObject obj) {
        _pendingInserts.add(obj);
        if (_pendingInserts.size() >= INSERT_BATCH_SIZE) {
            flushInserts();
        }
    }

    protected void update(SObject[] sObjects) {
        flushInserts();
        try {
            SaveResult[] saveResults = _connection.update(sObjects);
            checkSaveResults(saveResults, "update");
        } catch (ConnectionException e) {
            throw SalesforceUtils.wrapException(e, "Failed to update objects in Salesforce");
        }
    }

    @Override
    public void close() {
        flushInserts();
    }

    /**
     * Validates and builds a list of ID's referenced in a (potentially
     * composite) filter item. This is useful for both UPDATE and DELETE
     * operations in Salesforce, which are only supported by-id.
     * 
     * @param idList
     * @param whereItem
     */
    protected void buildIdList(List<String> idList, FilterItem whereItem) {
        if (whereItem.isCompoundFilter()) {
            final LogicalOperator logicalOperator = whereItem.getLogicalOperator();
            if (logicalOperator != LogicalOperator.OR) {
                throw new IllegalStateException(
                        "Salesforce only allows deletion of records by their specific IDs. Violated by operator between where items: "
                                + whereItem);
            }

            final FilterItem[] childItems = whereItem.getChildItems();
            for (FilterItem childItem : childItems) {
                buildIdList(idList, childItem);
            }
            return;
        }

        final OperatorType operator = whereItem.getOperator();

        if (!OperatorType.EQUALS_TO.equals(operator) && !OperatorType.IN.equals(operator)) {
            throw new IllegalStateException(
                    "Salesforce only allows deletion of records by their specific IDs. Violated by operator in where item: "
                            + whereItem);
        }

        final SelectItem selectItem = whereItem.getSelectItem();
        final Column column = selectItem.getColumn();

        final Object operand = whereItem.getOperand();

        if (column == null || operand == null || selectItem.getFunction() != null) {
            throw new IllegalStateException(
                    "Salesforce only allows deletion of records by their specific IDs. Violated by where item: "
                            + whereItem);
        }
        if (!column.isPrimaryKey()) {
            throw new IllegalStateException(
                    "Salesforce only allows deletion of records by their specific IDs. Violated by where item: "
                            + whereItem);
        }

        if (operand instanceof String) {
            idList.add((String) operand);
        } else if (operand instanceof List) {
            List<?> list = (List<?>) operand;
            for (Object object : list) {
                idList.add(object.toString());
            }
        } else if (operand instanceof String[]) {
            for (String str : (String[]) operand) {
                idList.add(str);
            }
        } else {
            throw new IllegalStateException(
                    "Salesforce only allows deletion of records by their specific IDs. Violated by operand in where item: "
                            + whereItem);
        }
    }
}
