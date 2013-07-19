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
package org.apache.metamodel.data;

import java.util.List;

import org.apache.metamodel.data.Style.Color;
import org.apache.metamodel.util.BaseObject;

final class ColorImpl extends BaseObject implements Color {
	
	private static final long serialVersionUID = 1L;

	private final short _red;
	private final short _green;
	private final short _blue;

	public ColorImpl(short red, short green, short blue) {
		checkRange(red);
		checkRange(green);
		checkRange(blue);
		_red = red;
		_green = green;
		_blue = blue;
	}

	private void checkRange(short rgbComponent) throws IllegalArgumentException {
		if (rgbComponent < 0 || rgbComponent > 255) {
			throw new IllegalArgumentException(
					"All RGB components must be between 0 and 255. Found: "
							+ rgbComponent);
		}
	}

	@Override
	public short getRed() {
		return _red;
	}

	@Override
	public short getGreen() {
		return _green;
	}

	@Override
	public short getBlue() {
		return _blue;
	}

	@Override
	protected void decorateIdentity(List<Object> identifiers) {
		identifiers.add(_red);
		identifiers.add(_green);
		identifiers.add(_blue);
	}

	@Override
	public String toString() {
		return "Color[" + _red + "," + _green + "," + _blue + "]";
	}
}
