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
package org.apache.metamodel.couchdb;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.DocumentOperationResult;
import org.apache.metamodel.AbstractUpdateCallback;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.delete.RowDeletionBuilder;
import org.apache.metamodel.drop.TableDropBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.RowUpdationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class CouchDbUpdateCallback extends AbstractUpdateCallback implements Closeable {

	private static final Logger logger = LoggerFactory.getLogger(CouchDbUpdateCallback.class);
	private final Map<String, CouchDbConnector> _connectors;

	public CouchDbUpdateCallback(CouchDbDataContext couchDbDataContext) {
		super(couchDbDataContext);
		_connectors = new HashMap<String, CouchDbConnector>();
	}

	@Override
	public CouchDbDataContext getDataContext() {
		return (CouchDbDataContext) super.getDataContext();
	}

	@Override
	public boolean isUpdateSupported() {
		return true;
	}

	@Override
	public RowUpdationBuilder update(Table table) throws IllegalArgumentException, IllegalStateException,
			UnsupportedOperationException {
		return new CouchDbRowUpdationBuilder(this, table);
	}

	@Override
	public boolean isCreateTableSupported() {
		return true;
	}

	@Override
	public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
			IllegalStateException {
		return new CouchDbTableCreationBuilder(this, schema, name);
	}

	@Override
	public boolean isDropTableSupported() {
		return true;
	}

	@Override
	public TableDropBuilder dropTable(Table table) throws IllegalArgumentException, IllegalStateException,
			UnsupportedOperationException {
		return new CouchDbTableDropBuilder(this, table);
	}

	@Override
	public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException,
			UnsupportedOperationException {
		return new CouchDbInsertionBuilder(this, table);
	}

	@Override
	public boolean isDeleteSupported() {
		return true;
	}

	@Override
	public RowDeletionBuilder deleteFrom(Table table) throws IllegalArgumentException, IllegalStateException,
			UnsupportedOperationException {
		return new CouchDbRowDeletionBuilder(this, table);
	}

	@Override
	public void close() {
		Collection<CouchDbConnector> connectorSet = _connectors.values();
		for (CouchDbConnector connector : connectorSet) {
			List<String> errornousResultsDescriptions = new ArrayList<String>();
			List<DocumentOperationResult> results = connector.flushBulkBuffer();
			for (DocumentOperationResult result : results) {
				if (result.isErroneous()) {
					String id = result.getId();
					String error = result.getError();
					String reason = result.getReason();
					String revision = result.getRevision();
					logger.error("Error occurred while flushing bulk buffer: {}, id: {}, revision: {}, reason: {}",
							new Object[] { error, id, revision, reason });
					errornousResultsDescriptions.add(error);
				}
			}

			if (!errornousResultsDescriptions.isEmpty()) {
				throw new MetaModelException(errornousResultsDescriptions.size() + " out of " + results.size()
						+ " operations in bulk was errornous: " + errornousResultsDescriptions);
			}
		}
	}

	public CouchDbConnector getConnector(String name) {
		CouchDbConnector connector = _connectors.get(name);
		if (connector == null) {
			CouchDbInstance instance = getDataContext().getCouchDbInstance();
			connector = instance.createConnector(name, false);
			_connectors.put(name, connector);
		}
		return connector;
	}

}
