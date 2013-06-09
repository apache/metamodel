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

package org.xBaseJ.indexes;

/**
 * This class is courtesy of the xBaseJ project: http://xbasej.sourceforge.net/
 * 
 * Copyright 1997-2007 - American Coders, LTD - Raleigh NC USA
 * 
 * <pre>
 * American Coders, Ltd
 * P. O. Box 97462
 * Raleigh, NC  27615  USA
 * 1-919-846-2014
 * http://www.americancoders.com
 * </pre>
 * 
 * @author Joe McVerry, American Coders Ltd.
 */
public class NodeFloat {
	private byte size;
	private byte sign;
	private byte value[];
	private double saveValue;

	public NodeFloat(byte invalue[]) {

		size = invalue[0];
		sign = invalue[1];

		byte neg1 = (byte) 0xd1;
		byte neg2 = (byte) 0xa9;

		value = new byte[10];
		int i;
		for (i = 0; i < 10; i++)
			value[i] = invalue[i + 2];

		if (sign == 0x10) {
			saveValue = 0.0;
			return;
		}

		boolean neg = false;

		if ((sign == neg1) || (sign == neg2))
			neg = true;

		byte b;
		char c1;
		StringBuffer sb = new StringBuffer(15);
		boolean negC;
		int i2;
		int j;
		for (j = 11; j > 0; j--) {
			if (invalue[j] != 0)
				break;
		}
		if (j == 1) {
			saveValue = 0.0;
			return;
		}

		j++;
		for (i = 2; i < j; i++) {
			b = invalue[i];
			negC = (b < 0);
			i2 = b & 0x70;
			i2 >>= 4;
			if (negC)
				i2 += 8;
			c1 = Integer.toString(i2).charAt(0);
			sb.append(c1);
			if (i == 2)
				sb.append('.');
			i2 = b & 0x0f;
			c1 = Integer.toString(i2).charAt(0);
			sb.append(c1);
		}
		sb.append('e');
		sb.append(Integer.toString(size - 0x35));
		String s = new String(sb);
		saveValue = Double.valueOf(s).doubleValue() * (neg ? -1 : 1);

		return;
	}

	public NodeFloat(double invalue) {

		size = 0x35;
		value = new byte[10];

		saveValue = invalue;

		int i;

		for (i = 0; i < 10; i++)
			value[i] = 0;

		if (invalue == 0.0) {
			sign = 0x10;
			return;
		}

		int bLine[] = { 0, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90 };
		int start = 0;

		boolean neg = (invalue < 0.0);
		if (neg)
			invalue *= -1.0;

		Double d = new Double(invalue);
		String s = d.toString();

		int epos = (s.indexOf('e') < 0 ? 0 : s.indexOf('e') + 1)
				+ (s.indexOf('E') < 0 ? 0 : s.indexOf('E') + 1);
		int decpos = s.indexOf('.');
		String s2;
		int t;
		if (epos > 0)
			s2 = s.substring(decpos + 1, epos - 1);
		else
			s2 = s.substring(decpos + 1);

		t = Integer.parseInt(s2);

		boolean decfound = (t > 0);

		if (neg)
			if (decfound)
				sign = (byte) 0xd1;
			else
				sign = (byte) 0xa9;
		else if (decfound)
			sign = (byte) 0x51;
		else
			sign = (byte) 0x29;

		if (epos > 0) {
			size += Integer.parseInt(s.substring(epos));
			s = s2;
		} else if ((decpos == 1) && (s.charAt(0) == '0')) {
			s = s2;
			size--;
			i = 2;
			while (i < s.length() && s.charAt(i) == '0') {
				size--;
				i++;
			}
			start = i;
		} else {
			size--;
			i = 0;
			while (s.charAt(i) != '.') {
				size++;
				i++;
			}
		}

		int v;
		start = 0;
		int j;
		char c;
		boolean top = true;
		for (i = start, j = 0; i < s.length() && j < 10; i++) {
			// first nibble
			c = s.charAt(i);

			if (c != '.')
				if (top) {
					v = Character.getNumericValue(c);
					value[j] = (byte) bLine[v];
					top = false;
				} else {
					// second nibble
					v = Character.getNumericValue(c);
					value[j] += v;
					top = true;
					j++;
				}
		}
		return;
	} // end of constructor

	public int compareTo(NodeFloat nf2) {

		if (sign == 0x10) {
			if (nf2.sign == 0x10)
				return 0;
			return nf2.sign < 0 ? -1 : 1; // nf2 is bigger if its > than 0;
		}

		if (nf2.sign == 0x10)
			return sign < 0 ? 1 : -1; // nf2 is bigger if its > than 0;

		if (sign < 0)
			if (nf2.sign > 0)
				return -1;

		if (sign > 0)
			if (nf2.sign < 0)
				return 1;

		// assume both the same sign

		if (size < nf2.size)
			return -1;
		if (size > nf2.size)
			return 1;

		for (int i = 0; i < 10; i++) {
			if (value[i] == nf2.value[i]) // they're equal go get next
				continue;

			if (value[i] < 0) // reverse all logic for bytes whose first nibble
			// is 8 or 9
			{
				if ((nf2.value[i] < 0) && (nf2.value[i] < value[i])) // though
					// it's
					// smaller
					// it's
					// bigger
					return -1;
				return 1;
			}
			if (nf2.value[i] < 0) // reverse all logic for bytes whose first
			// nibble is 8 or 9
			{
				return -1; // if nf2 is negative, we know this.value is not neg
				// so nf2 must be bigger.
			}
			if (value[i] < nf2.value[i])
				return -1;
			if (value[i] > nf2.value[i])
				return 1;
		}
		return 0;
	}

	public double getDoubleValue() {
		return saveValue;
	}

	public String toString() {
		return Double.toString(saveValue);
	}

	public byte[] getValue() {
		byte ret[] = new byte[12];
		ret[0] = size;
		ret[1] = sign;
		for (int i = 0; i < 10; i++)
			ret[i + 2] = value[i];
		return ret;
	}
} // end of class

