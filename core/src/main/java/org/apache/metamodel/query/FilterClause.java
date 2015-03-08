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

import java.util.List;

import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.schema.Column;

/**
 * Represents a clause of filters in the query. This type of clause is used for
 * the WHERE and HAVING parts of an SQL query.
 * 
 * Each provided FilterItem will be evaluated with the logical AND operator,
 * which requires that all filters are applied. Alternatively, if you wan't to
 * use an OR operator, then use the appropriate constructor of FilterItem to
 * create a composite filter.
 * 
 * @see FilterItem
 */
public class FilterClause extends AbstractQueryClause<FilterItem> {

    private static final long serialVersionUID = -9077342278766808934L;

    public FilterClause(Query query, String prefix) {
        super(query, prefix, AbstractQueryClause.DELIM_AND);
    }

    public List<SelectItem> getEvaluatedSelectItems() {
        final List<FilterItem> items = getItems();
        return MetaModelHelper.getEvaluatedSelectItems(items);
    }

    /**
     * Traverses the items and evaluates whether or not the given column is
     * referenced in either of them.
     * 
     * @param column
     * @return true if the column is referenced in the clause or false if not
     */
    public boolean isColumnReferenced(Column column) {
        for (FilterItem item : getItems()) {
            if (item.isReferenced(column)) {
                return true;
            }
        }
        return false;
    }
}