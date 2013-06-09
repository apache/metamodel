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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;

import org.xBaseJ.DBF;
import org.xBaseJ.DbaseUtils;
import org.xBaseJ.xBaseJException;
import org.xBaseJ.fields.Field;

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
public class NDX extends Index {

	public NDX() {
	}

	public NDX(String filename, DBF indatabase, char readonly)
			throws IOException, xBaseJException {
		int reading;
		int i;
		String wb;
		Node lNode = null;
		int Index_record;
		Field Field = null;
		dosname = filename;
		database = indatabase;

		file = new File(filename);

		if (!file.exists() || !file.isFile()) {
			throw new xBaseJException("Unknown Index file");
		} // /* endif */

		if (readonly == 'r')
			nfile = new RandomAccessFile(filename, "r");
		else
			nfile = new RandomAccessFile(filename, "rw");
		anchor_read();

		Index_record = top_Node;
		reading = Index_record;
		for (i = 0; i < 488; i++) {
			if (key_definition[i] <= ' ')
				break;
		}
		try {
			stringKey = new String(key_definition, 0, i, DBF.encodedType);
		} catch (UnsupportedEncodingException UEE) {
			stringKey = new String(key_definition, 0, i);
		}
		StringTokenizer strtok = new StringTokenizer(stringKey, "+ ");

		while (strtok.hasMoreElements()) {
			wb = (String) strtok.nextElement();
			wb = wb.trim();
			Field = database.getField(wb);
			keyControl.addElement(Field);
		} /* endwhile */

		while (reading > 0) {
			if (topNode == null) {
				lNode = new Node(nfile, key_per_Node, key_length, keyType,
						Index_record, false);
			} else {
				Node llNode = new Node(nfile, key_per_Node, key_length,
						keyType, Index_record, false);
				lNode.set_prev(llNode);
				lNode = (Node) llNode;
			} // /* endif */
			workNode = lNode;
			lNode.set_pos(0);
			lNode.read();

			if (reading > 0) {// /* if reading is zero we're still reading
				// Nodes, when < 0 then read
				// * a leaf */
				Index_record = lNode.get_lower_level();
				if (Index_record == 0) {
					Index_record = lNode.get_key_record_number();
					reading = 0; // /* read a leaf so last time in loop */
					lNode.set_pos(0);// /* so sequentially reads get first
					// record */
				}// /* Index = 0 then it's a leaf pointer */
			} // /* reading > 0 */
			if (topNode == null)
				topNode = (Node) lNode.clone();
		}// /* endwhile */

		if (topNode == null) {
			topNode = new Node(nfile, key_per_Node, key_length, keyType,
					Index_record, false);
			workNode = topNode;
		}
	}

	/**
	 * creates a NDX object and NDX file
	 * 
	 * @param name
	 *            Index file name , can be full or partial pathname
	 * @param NDXString
	 *            database Fields that define the Index, join Fields with "+" do
	 *            not use spaces
	 * @param indatabase
	 *            DBF object to be associated with
	 * @param destroy
	 *            permission to destroy file if it exists
	 * @param unique
	 *            unique flag indicator
	 * @throws IOException
	 *             exception thrown by calling methods
	 * @throws xBaseJException
	 *             most likely cause - file not found
	 */

	public NDX(String name, String NDXString, DBF indatabase, boolean destroy,
			boolean unique) throws xBaseJException, IOException {
		int len;
		String tempch1;
		char key_type = ' ';
		StringTokenizer strtok = new StringTokenizer(NDXString.toUpperCase(),
				"+");

		file = new File(name);
		dosname = name;
		database = indatabase;

		if (destroy == false)
			if (file.exists())
				throw new xBaseJException("NDX file already exists");

		if (destroy == true)
			if (file.exists())
				if (file.delete() == false)
					throw new xBaseJException("Can't delete old NDX file");

		key_length = 0;
		stringKey = new String(NDXString);
		set_key_definition(NDXString);
		unique_key = (byte) (unique ? 64 : 0);
		while (strtok.hasMoreElements()) {
			char type;
			tempch1 = (String) strtok.nextElement();
			Field Field = database.getField(tempch1);
			type = Field.getType();
			if (type == 'M')
				throw new xBaseJException("Can't make memo field part of a key");
			if (type == 'L')
				throw new xBaseJException(
						"Can't make logical ield part of a key");
			if (type == 'F')
				throw new xBaseJException(
						"Can't make float field part of a key");

			// the else portion of this can only get called when we
			// have more than one field making up the key.
			// It is ugly, but that's the way it is.
			//
			if (key_type == ' ')
				key_type = type;
			else if (key_type == 'D' && type == 'N')
				key_type = 'N';
			else if (key_type == 'N' && type == 'D')
				key_type = 'N'; // date key type really doesn't change
			else if (key_type != type)
				key_type = 'C';

			key_length += Field.getLength();
			keyControl.addElement(Field);
		}

		if (key_type == 'D' || key_type == 'N') {
			keyType = 'N';
			key_length = 8; // 20091007_rth
		} else {
			keyType = 'C';
		}

		len = (((key_length - 1) / 4) + 1) * 4;
		if (len < 1) {
			throw new xBaseJException("key length too short");
		} /* endif */
		if (len > 100) {
			throw new xBaseJException("key length too int");
		} /* endif */

		len += 8;
		next_available = 1;
		key_entry_size = (short) len;
		key_per_Node = (short) (509 / len);

		nfile = new RandomAccessFile(name, "rw");

		anchor_write();

		if (database.getRecordCount() > 0)
			bIndex();
		else {
			topNode = new Node(nfile, key_per_Node, key_length, keyType,
					next_available, false);
			workNode = topNode;
			topNode.set_pos(0);
			top_Node = next_available;
			next_available++;
			anchor_write();
			topNode.set_lower_level(0);
			topNode.set_key_record_number(0);
			topNode.set_keys_in_this_Node(0);
			topNode.write();
		}

	}

	public void close() throws IOException {
		nfile.close();
	}

	public int find_entry(NodeKey findKey) throws xBaseJException, IOException {
		return find_entry(findKey, findAnyKey);
	}

	public int find_entry(NodeKey findKey, int rec) throws xBaseJException,
			IOException {
		if (topNode == null) { /* no keys yet */
			throw new xBaseJException("No keys built");
		} /* endif */
		topNode.set_pos(0);
		record = find_entry(findKey, topNode, rec);
		return record;
	}

	public int find_entry(NodeKey findKey, Node aNode, int findrec)
			throws xBaseJException, IOException {
		foundExact = false;
		int rec, leaf, until;
		int stat = 1;
		Node Node_2;
		workNode = aNode;

		if (aNode == null) { /* no keys yet */
			throw new xBaseJException("No keys built");
		} /* endif */

		leaf = aNode.get_lower_level();
		if (leaf != 0) /*
						 * leaf pointers usually have one more pointer than
						 * shown
						 */
			until = aNode.get_keys_in_this_Node() + 1;
		else
			until = aNode.get_keys_in_this_Node();

		for (aNode.set_pos(0); aNode.get_pos() < until && stat > 0; aNode
				.pos_up()) {
			leaf = aNode.get_lower_level();
			rec = aNode.get_key_record_number();
			if (aNode.get_pos() < (aNode.get_keys_in_this_Node())) { /*
																	 * leafs
																	 * make us
																	 * do this
																	 */
				stat = findKey.compareKey(aNode.get_key_value());
				if (stat > 0)
					continue; // looping
			}

			if (leaf > 0) { /* still dealing with Nodes */
				if (aNode.get_next() == null) {
					Node_2 = new Node(nfile, key_per_Node, key_length, keyType,
							leaf, true);
					aNode.set_next(Node_2);
					Node_2.set_prev(aNode);
				} else
					Node_2 = aNode.get_next();
				Node_2.set_record_number(leaf);
				Node_2.read();
				Node_2.set_pos(0);
				workNode = Node_2;
				rec = find_entry(findKey, Node_2, findrec);
				return (rec); /* if rec < 0 then didn't find the record yet */
			} /* leaf > 0 */

			if (stat < 0) /* can't find the key but ... */
			{

				if (findrec > 0)
					return (keyNotFound); /*
										 * when findrec -1 then looking for
										 * specific key and record
										 */

				if (findrec == findFirstMatchingKey)
					return (keyNotFound); /*
										 * when findrec findAnyKey then for the
										 * key
										 */

				return (rec); /* looking for key greater than or equal to */
			}

			foundExact = true;
			/* stat is zero - key matches the current key */

			if ((findrec > 0) && (rec == findrec)) /*
													 * found matching key and
													 * matching record number
													 */
				return rec;

			if (findrec == findFirstMatchingKey)
				return rec; /* found one key that matches */

			if (findrec == findAnyKey)
				return rec; /* found one key that matches */

			/*
			 * findrec not zero so we are looking for the key that is greater
			 * than
			 */
			/* or we looking for a key with a particular record number */

		} /* end for */

		return (foundMatchingKeyButNotRecord); /*
												 * at end of current line but
												 * keep looking for recursion
												 */
	}

	public void bIndex() throws xBaseJException, IOException {
		int i;
		int reccount = database.getRecordCount();
		NodeKey lastkey;
		BinaryTree topTree = null;

		if (database.getRecordCount() > 0) {
			database.gotoRecord(1);
			top_Node = 0;
			next_available = 1;
			for (i = 1; i <= reccount; i++) {
				lastkey = build_key();
				if (topTree == null)
					topTree = new BinaryTree(lastkey, i, topTree);
				else
					new BinaryTree(lastkey, i, topTree);

				if (i < reccount)
					database.read();
			}

			topNode = null;

			reIndexWork(topTree.getLeast(), 0);

			anchor_write();

		}

		return;
	}

	public void reIndex() throws xBaseJException, IOException {
		int i;
		int reccount = database.getRecordCount();
		NodeKey lastkey;
		BinaryTree topTree = null;

		if (database.getRecordCount() > 0) {
			// database.gotoRecord(1); 20091010_rth
			top_Node = 0;
			next_available = 1;
			for (i = 1; i <= reccount; i++) {
				database.gotoRecord(i); // 20091010_rth
				lastkey = build_key();
				if (topTree == null)
					topTree = new BinaryTree(lastkey, i, topTree);
				else
					new BinaryTree(lastkey, i, topTree);

				// if (i < reccount) 20091010_rth
				// database.read();
			}

			topNode = null;
			nfile.close();
			file.delete();
			nfile = new RandomAccessFile(file, "rw");
			anchor_write();

			if (database.getRecordCount() > 0)
				reIndexWork(topTree.getLeast(), 0);

			anchor_write();

		}

		return;
	}

	private int reIndexWork(BinaryTree bt, int level) throws IOException,
			xBaseJException {

		BinaryTree tree2 = null;
		int pos = 0;
		top_Node = next_available;
		workNode = new Node(nfile, key_per_Node, key_length, keyType, top_Node,
				level > 0);
		next_available++;
		workNode.set_pos(0);
		NodeKey lastKey = null;
		btLoop: while (true) {
			if (pos == key_per_Node || bt == null) {
				if ((tree2 == null && pos == 1 && level > 0) || pos == 0) {
					top_Node--;
					next_available--;
					topNode = workNode; // just in case its not set
					for (int i = pos; i < key_per_Node; i++) {
						workNode.set_pos(i);
						workNode.set_key_value(lastKey);
					}
					workNode.write();
					break btLoop;
				}
				if (bt != null || tree2 != null) { // if bt not null more keys
					// to add and to upper node
					// if tree2 not nuul then
					// upper node needs the last
					// node
					if (tree2 == null) {
						topNode = workNode;
						tree2 = new BinaryTree(lastKey, workNode
								.get_record_number(), tree2);
					} else
						new BinaryTree(lastKey, workNode.get_record_number(),
								tree2);
				}
				if (level == 0)
					workNode.set_keys_in_this_Node(pos);
				else
					workNode.set_keys_in_this_Node(pos - 1);
				{
					for (int i = pos; i < key_per_Node; i++) {
						workNode.set_pos(i);
						workNode.set_key_value(lastKey);
					}
				}
				workNode.write();
				if (bt == null) { // we're all done get out of loop
					topNode = workNode; // just in case its not set
					break btLoop;
				}
				top_Node = next_available;
				workNode = new Node(nfile, key_per_Node, key_length, keyType,
						top_Node, level > 0);
				next_available++;
				pos = 0;
				workNode.set_pos(0);
			}
			pos++;
			lastKey = bt.getKey();
			workNode.set_key_value(lastKey);
			if (level == 0)
				workNode.set_key_record_number(bt.getWhere());
			else
				workNode.set_lower_level(bt.getWhere());
			workNode.pos_up();
			bt = bt.getNext();
		}

		if (tree2 == null)
			return 0;

		return reIndexWork(tree2.getLeast(), ++level);

	}

	public int add_entry(NodeKey NDXkey, int recno) throws xBaseJException,
			IOException {

		if (topNode != null) {
			find_entry(NDXkey, findAnyKey);
		}

		set_active_key(NDXkey);
		return update_entry(workNode, NDXkey, 0, 0, recno);

	}

	private int update_entry(Node aNode, NodeKey NDXkey, int leftleaf,
			int rightleaf, int recno) throws IOException, xBaseJException {

		int savepos;
		Node bNode;

		if (topNode == null) { /*
								 * we don't have any Index area yet so we must
								 * be adding the first record
								 */
			topNode = new Node(nfile, key_per_Node, key_length, keyType,
					next_available, false);
			workNode = topNode;
			topNode.set_pos(0);
			top_Node = next_available;
			next_available++;
			anchor_write();
			topNode.set_lower_level(0);
			topNode.set_key_record_number(recno);
			topNode.set_key_value(NDXkey);
			topNode.set_keys_in_this_Node(1);
			topNode.write();
			return 0;
		}

		/*
		 * this is flaky but if both leaf no or rec no are not zero then we are
		 * splitting the top Node
		 */
		/*
		 * for all other conditions if passed a leaf or a Node one of the two
		 * (leaf no or rec no)
		 */
		/* will be zero */
		if (leftleaf > 0 && recno > 0) { /* work to split the top Node */
			/* stuff should still reside in the old Node */

			bNode = new Node(nfile, key_per_Node, key_length, keyType,
					next_available, true);
			aNode.set_prev(bNode);

			bNode.set_next(aNode);

			bNode.set_pos(0);
			/* we want to get last one */
			bNode.set_lower_level(leftleaf);
			bNode.set_key_record_number(0);
			bNode.set_key_value(NDXkey);
			bNode.pos_up();
			bNode.set_lower_level(recno);
			bNode.set_key_record_number(0);

			/*
			 * now looks like a top Node
			 * leaf=value,rec#=0,key,leaf=0,rec#=0,empty
			 */

			topNode = (Node) bNode;
			bNode.set_keys_in_this_Node(1);

			top_Node = next_available;
			bNode.set_record_number(top_Node);
			next_available++;
			anchor_write();
			bNode.write();
			return 0;
		}

		savepos = aNode.get_pos();
		if (savepos < (aNode.get_keys_in_this_Node())) {
			/* add to middle of list */
			/*
			 * first move the record number of that trailing record indicator
			 * dbase III quirk
			 */
			int ptr, recn, i;
			NodeKey buf;
			i = aNode.get_keys_in_this_Node();
			aNode.set_pos(i);
			ptr = aNode.get_lower_level();
			aNode.pos_up();
			aNode.set_lower_level(ptr);
			/* then move all the other subNodes */
			// i--;
			aNode.set_pos(i); /* should be at last one in list */
			for (; // i is pointing to last Nodelet
			i > -1 && i >= savepos; i--) {
				ptr = aNode.get_lower_level();
				recn = aNode.get_key_record_number();
				buf = aNode.get_key_value();
				aNode.pos_up();
				aNode.set_lower_level(ptr);
				aNode.set_key_record_number(recn);
				aNode.set_key_value(buf);
				aNode.set_pos(i - 1);
			} /* endfor */
			aNode.set_pos(savepos); /* reposition after falling out */
			aNode.set_lower_level(leftleaf);
			aNode.set_key_record_number(recno);
			aNode.set_key_value(NDXkey);
			if (rightleaf > 0) {
				aNode.pos_up();
				aNode.set_lower_level(rightleaf);
				aNode.pos_down();
			}
		} else {
			aNode.set_pos(aNode.get_keys_in_this_Node());
			aNode.set_lower_level(leftleaf);
			aNode.set_key_record_number(recno);
			aNode.set_key_value(NDXkey);
			if (rightleaf > 0) {
				aNode.pos_up();
				aNode.set_lower_level(rightleaf);
				aNode.pos_down();
			}
		}
		aNode.set_keys_in_this_Node(aNode.get_keys_in_this_Node() + 1);
		aNode.write();

		if (aNode.get_keys_in_this_Node() >= key_per_Node)
			splitNode(aNode, savepos);

		return 0;
	}

	private void splitNode(Node aNode, int savepos) throws xBaseJException,
			IOException {

		int i, j, k;
		int left, right;
		Node bNode;
		/* this is where we do the famous split */
		/* first split one Node and preserve it on the file */
		/* then the new Node with half the old data can use the ending logic */
		/* which simply updates it in place */
		/* if pos < half way point in record */
		/* fix and write last (#allowed /2) +1 thru #allowed */
		/* split from 1 to #allowed /2 */
		/* add our new Index */
		/* write our new Index */
		/* add entry to Node above. last entry in our new Node */
		bNode = new Node(nfile, key_per_Node, key_length, keyType, 0, aNode
				.isBranch());
		bNode.set_pos(0);
		aNode.set_pos(0);
		for (k = 0; k < aNode.get_keys_in_this_Node(); k++) {
			bNode.set_lower_level(aNode.get_lower_level());
			bNode.set_key_record_number(aNode.get_key_record_number());
			bNode.set_key_value(aNode.get_key_value());
			bNode.pos_up();
			aNode.pos_up();
		} /* endfor */
		bNode.set_lower_level(aNode.get_lower_level());
		bNode.set_key_record_number(0);
		bNode.set_key_value("");
		i = aNode.get_keys_in_this_Node() / 2;
		j = aNode.get_keys_in_this_Node() - i;

		if (savepos > i) {
			bNode.set_keys_in_this_Node(i);
			if (aNode.get_next() != null)
				aNode.set_keys_in_this_Node(i - 1);
			else
				aNode.set_keys_in_this_Node(i); /* for top level split */
			left = aNode.get_record_number();
			aNode.write();
			right = next_available;
			next_available++;
			anchor_write();
			if (aNode.get_prev() != null) {
				bNode.set_pos(i - 1);
				update_entry((Node) aNode.get_prev(), bNode.get_key_value(),
						aNode.get_record_number(), right, 0);
			}
			aNode.set_pos(0);
			bNode.set_pos(i);
			for (k = 0; k <= j; k++) {
				aNode.set_lower_level(bNode.get_lower_level());
				aNode.set_key_record_number(bNode.get_key_record_number());
				aNode.set_key_value(bNode.get_key_value());
				aNode.pos_up();
				bNode.pos_up();
			}
			/* the right side is already one short */
			aNode.set_keys_in_this_Node(j);
			aNode.set_pos(savepos - i);
			aNode.set_record_number(right);
			aNode.write();
			if (aNode.get_prev() == null) {
				// create a new top Node
				bNode.set_pos(i - 1);
				// use new Node because it has the old data in it, at pos(i) is
				// the last key
				update_entry(bNode, bNode.get_key_value(), left, 0, right);
			}
		} else { /*
				 * create new second half and use the first half to put new key
				 * in
				 */
			right = aNode.get_record_number();
			aNode.set_pos(0);
			bNode.set_pos(j);
			for (k = 0; k <= i; k++) {
				aNode.set_lower_level(bNode.get_lower_level());
				aNode.set_key_record_number(bNode.get_key_record_number());
				aNode.set_key_value(bNode.get_key_value());
				aNode.pos_up();
				bNode.pos_up();
			}
			aNode.set_keys_in_this_Node(i);
			aNode.write();

			bNode.set_keys_in_this_Node(i);
			aNode.set_pos(0);
			bNode.set_pos(0);
			for (k = 0; k < key_per_Node; k++) {
				aNode.set_lower_level(bNode.get_lower_level());
				aNode.set_key_record_number(bNode.get_key_record_number());
				aNode.set_key_value(bNode.get_key_value());
				aNode.pos_up();
				bNode.pos_up();
			}

			aNode.set_record_number(next_available); // did a split renumber
			// Node
			next_available++;
			anchor_write();
			if (aNode.get_next() != null)
				aNode.set_keys_in_this_Node(j - 1);
			else
				aNode.set_keys_in_this_Node(j);

			aNode.set_pos(j - 1);
			aNode.write();
			left = aNode.get_record_number();
			if (aNode.get_prev() != null) {
				update_entry((Node) aNode.get_prev(), aNode.get_key_value(),
						left, right, 0);
			} else {
				bNode.set_pos(j - 1);
				update_entry(bNode, // pass it aNode for it 's creating a new
						// top Node
						bNode.get_key_value(), left, 0, right);
			}
		}
		bNode = null;
	} /* even if we did a split we don't exit because we still have to */

	public void del_entry(Node inNode) throws IOException, xBaseJException {

		Node aNode;
		int pos, k;

		aNode = inNode;

		pos = aNode.get_pos();
		k = pos;
		aNode.set_keys_in_this_Node(aNode.get_keys_in_this_Node() - 1);

		if (aNode.get_lower_level() != 0 // pointer node
				&& pos <= aNode.get_keys_in_this_Node()) {
			for (k = pos - 1; k < aNode.get_keys_in_this_Node(); k++) {
				int level, rec;
				NodeKey key;
				aNode.pos_up();
				level = aNode.get_lower_level();
				rec = aNode.get_key_record_number();
				key = aNode.get_key_value();
				aNode.pos_down();
				aNode.set_lower_level(level);
				aNode.set_key_record_number(rec);
				aNode.set_key_value(key);
				aNode.pos_up();
			} /* endfor */
		} /* endif */
		else if (pos < aNode.get_keys_in_this_Node()) // record node
		{
			for (k = pos; k < aNode.get_keys_in_this_Node(); k++) {
				int level, rec;
				NodeKey key;
				aNode.pos_up();
				level = aNode.get_lower_level();
				rec = aNode.get_key_record_number();
				key = aNode.get_key_value();
				aNode.pos_down();
				aNode.set_lower_level(level);
				aNode.set_key_record_number(rec);
				aNode.set_key_value(key);
				aNode.pos_up();
			} /* endfor */
		} /* endif */

		if (aNode.get_prev() != null) // should we fix parent?
		{
			if (aNode.get_keys_in_this_Node() == 0) {
				if (aNode.get_lower_level() == 0) // record node so go fix its
					// parent
					del_entry(aNode.get_prev());
				else
					; // pointer node so don't fix unless negative
			} else {
				if (aNode.get_keys_in_this_Node() == -1)
					del_entry(aNode.get_prev());
			}
		}

		aNode.set_pos(pos);
		aNode.write();
	}

	public int get_next_key() throws xBaseJException, IOException {
		return get_next_key(workNode);
	}

	private int get_next_key(Node aNode) throws xBaseJException, IOException {

		int rec, until, leaf;

		if (aNode == null)
			return -1;

		aNode.pos_up();

		leaf = aNode.get_lower_level();

		if (leaf > 0) /* leaf pointers usually have one more pointer than shown */
			until = aNode.get_keys_in_this_Node() + 1;
		else
			until = aNode.get_keys_in_this_Node();

		if (aNode.get_pos() >= until) {
			Node rNode;
			rNode = aNode.get_prev();
			rec = get_next_key(rNode);
			if (rec == -1) {
				aNode.set_pos(until);
				return -1;
			} /* endif */
			workNode = aNode;
			aNode.set_record_number(rec);
			aNode.read();
			aNode.set_pos(0);
		} /* endif */

		leaf = aNode.get_lower_level();
		workNode = aNode;
		if (leaf > 0)
			return (leaf);
		return (aNode.get_key_record_number());
	}

	public int get_prev_key() throws xBaseJException, IOException {
		return get_prev_key(workNode);
	}

	private int get_prev_key(Node aNode) throws xBaseJException, IOException {
		int rec, until, leaf;

		if (aNode == null)
			return -1;

		if (aNode.get_pos() < 0)
			return -1;

		leaf = aNode.get_lower_level();

		if (leaf > 0) /* leaf pointers usually have one more pointer than shown */
			until = 1;
		else
			until = 0;

		if (aNode.get_pos() > -1)
			aNode.pos_down();

		if (aNode.get_pos() < 0) {

			rec = get_prev_key(aNode.get_prev());
			if (rec == -1) {
				return -1;
			}
			aNode.set_record_number(rec);
			aNode.read();
			aNode.set_pos(aNode.get_keys_in_this_Node() + until);
			aNode.pos_down(); /* offset at zero not 1 */
		} /* endif */

		leaf = aNode.get_lower_level();
		workNode = aNode;
		if (leaf > 0) {
			return (leaf);
		}
		return aNode.get_key_record_number();
	}

	public void anchor_read() throws IOException {
		nfile.seek(0);
		top_Node = nfile.readInt();
		next_available = nfile.readInt();
		reserved_02 = nfile.readInt();
		key_length = nfile.readShort();
		key_per_Node = nfile.readShort();
		keyType = (nfile.readShort() != 0) ? 'N' : 'C';
		key_entry_size = nfile.readShort();
		reserved_01 = nfile.readByte();
		reserved_03 = nfile.readByte();
		reserved_04 = nfile.readByte();
		unique_key = nfile.readByte();
		nfile.readFully(key_definition, 0, 488);
		redo_numbers();
	}

	public void anchor_write() throws IOException {
		nfile.seek(0);
		redo_numbers();
		nfile.writeInt(top_Node);
		nfile.writeInt(next_available);
		nfile.writeInt(reserved_02);
		nfile.writeShort(key_length);
		nfile.writeShort(key_per_Node);
		nfile.writeShort(keyType == 'N' ? 1 : 0);
		nfile.writeShort(key_entry_size);
		nfile.writeByte(reserved_01);
		nfile.writeByte(reserved_03);
		nfile.writeByte(reserved_04);
		nfile.writeByte(unique_key);
		nfile.write(key_definition, 0, 488);
		redo_numbers();
	}

	public void redo_numbers() {
		top_Node = DbaseUtils.x86(top_Node);
		next_available = DbaseUtils.x86(next_available);
		key_length = DbaseUtils.x86(key_length);
		key_per_Node = DbaseUtils.x86(key_per_Node);
		// keyType = Util.x86(keyType);
		key_entry_size = DbaseUtils.x86(key_entry_size);
	}

}
