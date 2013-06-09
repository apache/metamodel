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
package org.eobjects.metamodel.couchdb;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.DocumentOperationResult;
import org.eobjects.metamodel.AbstractUpdateCallback;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.create.TableCreationBuilder;
import org.eobjects.metamodel.delete.RowDeletionBuilder;
import org.eobjects.metamodel.drop.TableDropBuilder;
import org.eobjects.metamodel.insert.RowInsertionBuilder;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.update.RowUpdationBuilder;
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
