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
package org.apache.metamodel.query.builder;

import java.util.List;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.JoinType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

final class JoinFromBuilderImpl extends SatisfiedFromBuilderCallback implements JoinFromBuilder {

    private JoinType joinType;
    private FromItem leftItem;
    private FromItem rightItem;

    public JoinFromBuilderImpl(Query query, FromItem leftItem, Table rightTable, JoinType joinType,
            DataContext dataContext) {
        super(query, dataContext);
        this.joinType = joinType;
        this.leftItem = leftItem;
        this.rightItem = new FromItem(rightTable);
    }

    @Override
    public SatisfiedFromBuilder on(String left, String right) throws IllegalArgumentException {
        Table leftTable = leftItem.getTable();
        if (leftTable == null) {
            throw new IllegalArgumentException("Left side of join is not a Table, cannot resolve ON item: '" + left + "'.");
        }
        Table rightTable = rightItem.getTable();
        if (rightTable == null) {
            throw new IllegalArgumentException("Right side of join is not a Table, cannot resolve ON item: '" + right + "'.");
        }

        Column leftColumn = leftTable.getColumnByName(left);
        Column rightColumn = rightTable.getColumnByName(right);
        return on(leftColumn, rightColumn);
    }

    @Override
    public SatisfiedFromBuilder on(Column left, Column right) throws IllegalArgumentException {
        if (left == null) {
            throw new IllegalArgumentException("left cannot be null");
        }
        if (right == null) {
            throw new IllegalArgumentException("right cannot be null");
        }
        getQuery().getFromClause().removeItem(leftItem);

        SelectItem[] leftOn = new SelectItem[] { new SelectItem(left) };
        SelectItem[] rightOn = new SelectItem[] { new SelectItem(right) };
        FromItem fromItem = new FromItem(joinType, leftItem, rightItem, leftOn, rightOn);

        getQuery().from(fromItem);

        return this;
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        super.decorateIdentity(identifiers);
        identifiers.add(joinType);
        identifiers.add(leftItem);
        identifiers.add(rightItem);
    }
}