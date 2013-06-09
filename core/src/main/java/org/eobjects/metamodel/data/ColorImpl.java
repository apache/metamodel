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
package org.eobjects.metamodel.data;

import java.util.List;

import org.eobjects.metamodel.data.Style.Color;
import org.eobjects.metamodel.util.BaseObject;

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
