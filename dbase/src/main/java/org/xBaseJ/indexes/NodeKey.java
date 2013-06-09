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
public class NodeKey {
	char type = ' ';
	Object key;

	public NodeKey(Object keyIn) {

		if (keyIn instanceof String)
			type = 'C';
		else if (keyIn instanceof Double)
			type = 'N';
		else if (keyIn instanceof NodeFloat)
			type = 'F';

		key = keyIn;

	}

	public char getType() {
		return type;
	}

	public String rebuildString(String inString) {
		char a[] = new char[inString.length()];
		for (int i = 0; i < inString.length(); i++) {
			if (inString.charAt(i) == '_')
				a[i] = 31;
			else
				a[i] = inString.charAt(i);
		}

		return new String(a);
	}

	public int compareKey(NodeKey keyCompareTo) // throws new xBaseJException
	{
		int ret = 0;
		if (type != keyCompareTo.getType())
			return -1; // throw new
		// xBaseJException("Node key types do not match");
		if (type == 'C') {
			String s = (String) key;
			s = rebuildString(s);
			String t = keyCompareTo.toString();
			t = rebuildString(t);
			return s.compareTo(t);
		}
		if (type == 'F') {
			NodeFloat nf = (NodeFloat) key;
			NodeFloat nft = (NodeFloat) keyCompareTo.key;
			return nf.compareTo(nft);
		}
		Double d = (Double) key;

		double d2 = d.doubleValue() - keyCompareTo.toDouble();
		if (d2 < 0.0)
			return -1;
		if (d2 > 0.0)
			return 1;
		return ret;
	}

	public int length() {
		if (type == 'C')
			return ((String) key).length();
		if (type == 'F')
			return 12;
		return 8;
	}

	public String toString() {
		return key.toString();
	}

	public double toDouble() {
		if (type == 'N') {
			Double d = (Double) key;
			return d.doubleValue();
		}
		return 0.0;
	}

	public NodeFloat toNodeFloat() {
		if (type == 'F') {
			NodeFloat f = (NodeFloat) key;
			return f;
		}
		return null;
	}

}
