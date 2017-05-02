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
package org.apache.metamodel.query;

import java.io.Serializable;
import java.util.List;

public interface QueryClause<E> extends Serializable {

    public QueryClause<E> setItems(@SuppressWarnings("unchecked") E... items);

    public QueryClause<E> addItems(@SuppressWarnings("unchecked") E... items);

    public QueryClause<E> addItems(Iterable<E> items);

    public QueryClause<E> addItem(int index, E item);

    public QueryClause<E> addItem(E item);

    public boolean isEmpty();

    public int getItemCount();

    public E getItem(int index);

    public List<E> getItems();

    public QueryClause<E> removeItem(int index);

    public QueryClause<E> removeItem(E item);

    public QueryClause<E> removeItems();

    public String toSql(boolean includeSchemaInColumnPaths);

    public String toSql();

    public int indexOf(E item);
}
