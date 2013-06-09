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

import org.eobjects.metamodel.util.AggregateBuilder;
import org.eobjects.metamodel.util.NumberComparator;

final class AverageAggregateBuilder implements AggregateBuilder<Double> {

	public double _average;
	public int _numValues;

	@Override
	public void add(Object o) {
		if (o == null) {
			return;
		}
		Number number = NumberComparator.toNumber(o);
		if (number == null) {
			throw new IllegalArgumentException("Could not convert to number: "
					+ o);
		}
		double total = _average * _numValues + number.doubleValue();
		_numValues++;
		_average = total / _numValues;
	}

	@Override
	public Double getAggregate() {
		return _average;
	}

}
