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
package org.eobjects.metamodel.util;

import java.text.DateFormat;
import java.util.Date;

/**
 * A Date implementation that is immutable and has a predictable
 * (locale-indifferent) toString() method.
 * 
 * @author Kasper Sørensen
 * 
 * @deprecated MetaModel is not a Date API, use Joda time or live with
 *             java.util.Date.
 */
@Deprecated
public final class ImmutableDate extends Date {

	private static final long serialVersionUID = 1L;

	public ImmutableDate(long time) {
		super(time);
	}

	public ImmutableDate(Date date) {
		super(date.getTime());
	}

	/**
	 * This mutator will throw an {@link UnsupportedOperationException}, since
	 * the date is ummutable.
	 * 
	 * @param time
	 *            new time to set
	 */
	@Override
	public void setTime(long time) {
		throw new UnsupportedOperationException("setTime(...) is not allowed");
	}

	@Override
	public String toString() {
		DateFormat format = DateUtils.createDateFormat();
		return format.format(this);
	}
}
