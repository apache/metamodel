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

import java.io.ObjectStreamException;
import java.util.List;

import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.TableType;

/**
 * Table implementation that is based on JDBC metadata.
 */
final class JdbcTable extends MutableTable {

	private static final long serialVersionUID = 5952310469458880330L;

	private final transient MetadataLoader _metadataLoader;

	public JdbcTable(String name, TableType type, JdbcSchema schema, MetadataLoader metadataLoader) {
		super(name, type, schema);
		_metadataLoader = metadataLoader;
	}

	@Override
	protected List<Column> getColumnsInternal() {
		if (_metadataLoader != null) {
			_metadataLoader.loadColumns(this);
		}
		return super.getColumnsInternal();
	}

	@Override
	protected List<Relationship> getRelationshipsInternal() {
		Schema schema = getSchema();
		if (schema instanceof JdbcSchema) {
			((JdbcSchema) schema).loadRelations();
		}
		return super.getRelationshipsInternal();
	}
	
	protected void loadIndexes() {
		if (_metadataLoader != null) {
			_metadataLoader.loadIndexes(this);
		}
	}

	/**
	 * Called by the Java Serialization API to serialize the object.
	 */
	private Object writeReplace() throws ObjectStreamException {
		getColumns();
		loadIndexes();
		loadPrimaryKeys();
		return this;
	}

	public void loadPrimaryKeys() {
		if (_metadataLoader != null) {
			_metadataLoader.loadPrimaryKeys(this);
		}
	}
}