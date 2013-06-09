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
 * An immutable implementation of the Table interface.
 * 
 * It is not intended to be instantiated on it's own. Rather, use the
 * constructor in ImmutableSchema.
 * 
 * @see ImmutableSchema
 * 
 * @author Kasper Sørensen
 */
final class ImmutableTable extends AbstractTable implements Serializable {

	private static final long serialVersionUID = 1L;

	private final List<ImmutableColumn> columns = new ArrayList<ImmutableColumn>();
	private final List<ImmutableRelationship> relationships = new ArrayList<ImmutableRelationship>();
	private final ImmutableSchema schema;
	private final TableType type;
	private final String remarks;
	private final String name;
	private final String quote;

	protected ImmutableTable(String name, TableType type, ImmutableSchema schema,
			String remarks, String quote) {
		this.name = name;
		this.type = type;
		this.schema = schema;
		this.remarks = remarks;
		this.quote = quote;
	}

	protected ImmutableTable(Table table, ImmutableSchema schema) {
		this(table.getName(), table.getType(), schema, table.getRemarks(),
				table.getQuote());
		Column[] origColumns = table.getColumns();
		for (Column column : origColumns) {
			columns.add(new ImmutableColumn(column, this));
		}
	}

	protected void addRelationship(ImmutableRelationship relationship) {
		if (!relationships.contains(relationship)) {
			relationships.add(relationship);
		}
	}

	@Override
	public Column[] getColumns() {
		return columns.toArray(new Column[columns.size()]);
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public TableType getType() {
		return type;
	}

	@Override
	public Relationship[] getRelationships() {
		return relationships.toArray(new Relationship[relationships.size()]);
	}

	@Override
	public String getRemarks() {
		return remarks;
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
