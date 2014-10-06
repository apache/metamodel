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
package org.apache.metamodel.util;

import java.text.DateFormat;
import java.util.Date;

/**
 * A Date implementation that is immutable and has a predictable
 * (locale-indifferent) toString() method.
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
