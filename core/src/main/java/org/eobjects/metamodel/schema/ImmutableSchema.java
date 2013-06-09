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
package org.eobjects.metamodel.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * An immutable implementation of the {@link Schema} interface.
 * 
 * @author Kasper Sørensen
 */
public final class ImmutableSchema extends AbstractSchema implements
		Serializable {

	private static final long serialVersionUID = 1L;

	private final List<ImmutableTable> tables = new ArrayList<ImmutableTable>();
	private String name;
	private String quote;

	private ImmutableSchema(String name, String quote) {
		super();
		this.name = name;
		this.quote = quote;
	}

	public ImmutableSchema(Schema schema) {
		this(schema.getName(), schema.getQuote());
		Table[] origTables = schema.getTables();
		for (Table table : origTables) {
			tables.add(new ImmutableTable(table, this));
		}

		Relationship[] origRelationships = schema.getRelationships();
		for (Relationship relationship : origRelationships) {
			ImmutableRelationship.create(relationship, this);
		}
	}

	@Override
	public Table[] getTables() {
		return tables.toArray(new Table[tables.size()]);
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String getQuote() {
		return quote;
	}
}
