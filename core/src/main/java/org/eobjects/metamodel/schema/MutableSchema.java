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
import java.util.Collection;
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
	public MutableTable[] getTables() {
		MutableTable[] array = new MutableTable[_tables.size()];
		return _tables.toArray(array);
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