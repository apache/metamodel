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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectInputStream.GetField;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.metamodel.util.LegacyDeserializationObjectInputStream;

public final class ImmutableRelationship extends AbstractRelationship implements Serializable {

	private static final long serialVersionUID = 1L;

	private final List<Column> primaryColumns;
	private final List<Column> foreignColumns;

	public static void create(Relationship origRelationship,
			ImmutableSchema schema) {
		ImmutableTable primaryTable = getSimilarTable(
				origRelationship.getPrimaryTable(), schema);
		assert primaryTable != null;
		List<Column> primaryColumns = getSimilarColumns(
				origRelationship.getPrimaryColumns(), primaryTable);
		checkSameTable(primaryColumns);

		ImmutableTable foreignTable = getSimilarTable(
				origRelationship.getForeignTable(), schema);
		assert foreignTable != null;
		List<Column> foreignColumns = getSimilarColumns(
				origRelationship.getForeignColumns(), foreignTable);
		checkSameTable(foreignColumns);

		ImmutableRelationship relationship = new ImmutableRelationship(
				primaryColumns, foreignColumns);
		primaryTable.addRelationship(relationship);
		foreignTable.addRelationship(relationship);
	}

	private static List<Column> getSimilarColumns(List<Column> columns, Table table) {
		return columns.stream()
				.map( col -> table.getColumnByName(col.getName()))
				.collect(Collectors.toList());

	}

	private static ImmutableTable getSimilarTable(Table table,
			ImmutableSchema schema) {
		String name = table.getName();
		return (ImmutableTable) schema.getTableByName(name);
	}

	private ImmutableRelationship(List<Column> primaryColumns,
			List<Column> foreignColumns) {
		this.primaryColumns = primaryColumns;
		this.foreignColumns = foreignColumns;
	}

	@Override
	public List<Column> getPrimaryColumns() {
		return primaryColumns;
	}

	@Override
	public List<Column> getForeignColumns() {
		return foreignColumns;
	}

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        final GetField getFields = stream.readFields();
        Object primaryColumns = getFields.get("primaryColumns", null);
        Object foreignColumns = getFields.get("foreignColumns", null);
        if (primaryColumns instanceof Column[] && foreignColumns instanceof Column[]) {
            primaryColumns = Arrays.<Column> asList((Column[]) primaryColumns);
            foreignColumns = Arrays.<Column> asList((Column[]) foreignColumns);
        }
        LegacyDeserializationObjectInputStream.setField(ImmutableRelationship.class, this, "primaryColumns",
                primaryColumns);
        LegacyDeserializationObjectInputStream.setField(ImmutableRelationship.class, this, "foreignColumns",
                foreignColumns);
    }
}
