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
public class BinaryTree extends Object {
	private BinaryTree lesser;
	private BinaryTree greater;
	private BinaryTree above;
	private NodeKey key;
	private int where;

	NodeKey getKey() {
		return key;
	}

	int getWhere() {
		return where;
	}

	private void setLesser(BinaryTree inTree) {
		lesser = inTree;
	}

	private void setGreater(BinaryTree inTree) {
		greater = inTree;
	}

	public BinaryTree(NodeKey inkey, int inWhere, BinaryTree top) {
		above = null;
		lesser = null;
		greater = null;
		key = inkey;
		where = inWhere;

		if (top != null) {
			above = top.findPos(key);
			if (above.getKey().compareKey(inkey) > 0)
				above.setLesser(this);
			else
				above.setGreater(this);
		}

	}

	private BinaryTree findPos(NodeKey inkey) {

		if (key.compareKey(inkey) > 0)
			if (lesser == null)
				return this;
			else
				return (lesser.findPos(inkey));
		else if (greater == null)
			return this;
		return (greater.findPos(inkey));
	}

	public BinaryTree getLeast() {
		if (lesser != null) {
			return (lesser.getLeast());
		}
		return this;
	}

	public BinaryTree getNext() {
		if (greater == null)
			if (above == null)
				return null;
			else
				return above.goingUp(key);
		return greater.getLeast();
	}

	private BinaryTree goingUp(NodeKey inKey) {
		if (key.compareKey(inKey) <= 0)
			if (above == null)
				return null;
			else
				return above.goingUp(key);
		return this;
	}

}
