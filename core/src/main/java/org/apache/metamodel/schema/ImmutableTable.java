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
package org.apache.metamodel.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * An immutable implementation of the Table interface.
 * 
 * It is not intended to be instantiated on it's own. Rather, use the
 * constructor in ImmutableSchema.
 * 
 * @see ImmutableSchema
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
		List<Column> origColumns = table.getColumns();
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
	public List<Column> getColumns() {
		return Collections.unmodifiableList(columns);
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
	public Collection<Relationship> getRelationships() {
		return Collections.unmodifiableCollection(relationships);
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
