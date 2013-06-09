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
package org.eobjects.metamodel.query;

import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.util.AggregateBuilder;

/**
 * Represents an aggregate function to use in a SelectItem.
 * 
 * @see SelectItem
 */
public enum FunctionType {

    COUNT {
        @Override
        public AggregateBuilder<Long> build() {
            return new CountAggregateBuilder();
        }
    },
    AVG {
        @Override
        public AggregateBuilder<Double> build() {
            return new AverageAggregateBuilder();
        }
    },
    SUM {
        @Override
        public AggregateBuilder<Double> build() {
            return new SumAggregateBuilder();
        }
    },
    MAX {
        @Override
        public AggregateBuilder<Object> build() {
            return new MaxAggregateBuilder();
        }
    },
    MIN {
        @Override
        public AggregateBuilder<Object> build() {
            return new MinAggregateBuilder();
        }
    };

    public ColumnType getExpectedColumnType(ColumnType type) {
        switch (this) {
        case COUNT:
            return ColumnType.BIGINT;
        case AVG:
        case SUM:
            return ColumnType.DOUBLE;
        case MAX:
        case MIN:
            return type;
        default:
            return type;
        }
    }

    public SelectItem createSelectItem(Column column) {
        return new SelectItem(this, column);
    }

    public SelectItem createSelectItem(String expression, String alias) {
        return new SelectItem(this, expression, alias);
    }

    public Object evaluate(Iterable<?> values) {
        AggregateBuilder<?> builder = build();
        for (Object object : values) {
            builder.add(object);
        }
        return builder.getAggregate();
    }

    /**
     * Executes the function
     * 
     * @param values
     *            the values to be evaluated. If a value is null it won't be
     *            evaluated
     * @return the result of the function execution. The return type class is
     *         dependent on the FunctionType and the values provided. COUNT
     *         yields a Long, AVG and SUM yields Double values and MAX and MIN
     *         yields the type of the provided values.
     */
    public Object evaluate(Object... values) {
        AggregateBuilder<?> builder = build();
        for (Object object : values) {
            builder.add(object);
        }
        return builder.getAggregate();
    }

    public abstract AggregateBuilder<?> build();

    public static FunctionType get(String functionName) {
        try {
            return valueOf(functionName);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}