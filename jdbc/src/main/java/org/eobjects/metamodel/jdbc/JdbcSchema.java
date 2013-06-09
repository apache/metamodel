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

import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;

/**
 * Schema implementation for JDBC data contexts
 * 
 * @author Kasper Sørensen
 */
final class JdbcSchema extends MutableSchema {

	private static final long serialVersionUID = 7543633400859277467L;
	private transient MetadataLoader _metadataLoader;

	public JdbcSchema(String name, MetadataLoader metadataLoader) {
		super(name);
		_metadataLoader = metadataLoader;
	}

	protected void refreshTables() {
		if (_metadataLoader != null) {
			_metadataLoader.loadTables(this);
		}
	}

	public void loadRelations() {
		if (_metadataLoader != null) {
			_metadataLoader.loadRelations(this);
		}
	}

	public Schema toSerializableForm() {
		MutableTable[] tables = getTables();
		for (MutableTable table : tables) {
			table.getColumns();
			table.getIndexedColumns();
			table.getPrimaryKeys();
		}
		loadRelations();
		return this;
	}

	/**
	 * Called by the Java Serialization API to serialize the object.
	 */
	private Object writeReplace() throws ObjectStreamException {
		return toSerializableForm();
	}
}