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

import java.io.ObjectStreamException;
import java.util.List;

import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Relationship;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.TableType;

/**
 * Table implementation that is based on JDBC metadata.
 * 
 * @author Kasper Sørensen
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