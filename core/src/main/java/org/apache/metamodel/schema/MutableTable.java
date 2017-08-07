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
 * Represents the metadata about a table. Tables reside within a schema and
 * contains columns and relationships to other tables.
 * 
 * @see Schema
 * @see Column
 * @see Relationship
 */
public class MutableTable extends AbstractTable implements Serializable {

    private static final long serialVersionUID = -5094888465714027354L;
    protected String _name;
    protected TableType _type;
    protected String _remarks;
    protected Schema _schema;
    protected final List<Column> _columns;
    protected final List<Relationship> _relationships;
    protected String _quoteString = null;

    public MutableTable() {
        super();
        _columns = new ArrayList<Column>();
        _relationships = new ArrayList<Relationship>();
    }

    public MutableTable(String name) {
        this();
        setName(name);
    }

    public MutableTable(String name, TableType type) {
        this(name);
        _type = type;
    }

    public MutableTable(String name, TableType type, Schema schema) {
        this(name, type);
        _schema = schema;
    }

    public MutableTable(String name, TableType type, Schema schema, Column... columns) {
        this(name, type, schema);
        setColumns(columns);
    }

    public MutableTable(String name, Schema schema) {
        this(name);
        setSchema(schema);
    }

    @Override
    public String getName() {
        return _name;
    }

    public MutableTable setName(String name) {
        _name = name;
        return this;
    }

    /**
     * Internal getter for the columns of the table. Overwrite this method to
     * implement column lookup, column lazy-loading or similar.
     */
    protected List<Column> getColumnsInternal() {
        return _columns;
    }

    /**
     * Internal getter for the relationships of the table. Overwrite this method
     * to implement relationship lookup, relationship lazy-loading or similar.
     */
    protected List<Relationship> getRelationshipsInternal() {
        return _relationships;
    }

    @Override
    public List<Column> getColumns() {
        List<Column> columns = getColumnsInternal();
        return Collections.unmodifiableList(columns);
    }

    public MutableTable setColumns(Column... columns) {
        _columns.clear();
        for (Column column : columns) {
            _columns.add(column);
        }
        return this;
    }

    public MutableTable setColumns(Collection<Column> columns) {
        _columns.clear();
        for (Column column : columns) {
            _columns.add(column);
        }
        return this;
    }

    public MutableTable addColumn(Column column) {
        _columns.add(column);
        return this;
    }

    public MutableTable addColumn(int index, Column column) {
        _columns.add(index, column);
        return this;
    }

    public MutableTable removeColumn(Column column) {
        _columns.remove(column);
        return this;
    }

    @Override
    public Schema getSchema() {
        return _schema;
    }

    public MutableTable setSchema(Schema schema) {
        _schema = schema;
        return this;
    }

    @Override
    public TableType getType() {
        return _type;
    }

    public MutableTable setType(TableType type) {
        _type = type;
        return this;
    }

    @Override
    public List<Relationship> getRelationships() {
        //init relationships
        getRelationshipsInternal();
        return Collections.unmodifiableList(_relationships);
    }

    /**
     * Protected method for adding a relationship to this table. Should not be
     * used. Use Relationship.createRelationship(Column[], Column[]) instead.
     */
    protected void addRelationship(Relationship relation) {
        for (Relationship existingRelationship : _relationships) {
            if (existingRelationship.equals(relation)) {
                // avoid duplicate relationships
                return;
            }
        }
        _relationships.add(relation);
    }

    /**
     * Protected method for removing a relationship from this table. Should not
     * be used. Use Relationship.remove() instead.
     */
    protected MutableTable removeRelationship(Relationship relation) {
        _relationships.remove(relation);
        return this;
    }

    @Override
    public String getRemarks() {
        return _remarks;
    }

    public MutableTable setRemarks(String remarks) {
        _remarks = remarks;
        return this;
    }

    @Override
    public String getQuote() {
        return _quoteString;
    }

    public MutableTable setQuote(String quoteString) {
        _quoteString = quoteString;
        return this;
    }
}