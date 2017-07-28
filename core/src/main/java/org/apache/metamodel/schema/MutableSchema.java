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
 * Represents a schema and it's metadata. Schemas represent a collection of
 * tables.
 * 
 * @see Table
 */
public class MutableSchema extends AbstractSchema implements Serializable,
		Schema {

	private static final long serialVersionUID = 4465197783868238863L;

	private String _name;
	private final List<MutableTable> _tables;

	public MutableSchema() {
		super();
		_tables = new ArrayList<MutableTable>();
	}

	public MutableSchema(String name) {
		this();
		_name = name;
	}

	public MutableSchema(String name, MutableTable... tables) {
		this(name);
		setTables(tables);
	}

	@Override
	public String getName() {
		return _name;
	}

	public MutableSchema setName(String name) {
		_name = name;
		return this;
	}

	@Override
	public List<Table> getTables() {
		return Collections.unmodifiableList(_tables);
	}


	public MutableSchema setTables(Collection<? extends MutableTable> tables) {
	    clearTables();
		for (MutableTable table : tables) {
			_tables.add(table);
		}
		return this;
	}

	public MutableSchema setTables(MutableTable... tables) {
	    clearTables();
		for (MutableTable table : tables) {
			_tables.add(table);
		}
		return this;
	}
	
	public MutableSchema clearTables() {
	    _tables.clear();
	    return this;
	}

	public MutableSchema addTable(MutableTable table) {
		_tables.add(table);
		return this;
	}

	public MutableSchema removeTable(Table table) {
		_tables.remove(table);
		return this;
	}

	@Override
	public String getQuote() {
		return null;
	}
}