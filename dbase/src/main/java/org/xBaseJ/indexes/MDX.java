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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;

import org.xBaseJ.DBF;
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
public class MDX extends Index {

	short tag_pos;
	private MDXFile mfile;
	TagDescriptor tagDesc;
	TagHeader tagHead;

	public MDX(MDXFile ifile, DBF iDBF, short ipos) throws xBaseJException,
			IOException {
		int reading;

		String wb;
		MNode lNode = null;
		int Index_record;
		Field field = null;
		database = iDBF;
		mfile = ifile;
		tagDesc = mfile.getTagDescriptor(ipos);
		dosname = tagDesc.getName();
		tagHead = new TagHeader(ifile, (short) tagDesc.indheaderpage);
		key_per_Node = tagHead.key_per_Node;
		key_length = tagHead.key_length;
		nfile = ifile.getRandomAccessFile();
		keyType = (char) tagHead.keyType;

		if ((keyType == 'N') && (key_length == 12))
			keyType = 'F'; // now flip to xbasej definition of.
		if (keyType == 'D')
			keyType = 'N';

		Index_record = (int) tagHead.top_Node;

		reading = Index_record;
		stringKey = new String(tagHead.NDXString);
		StringTokenizer strtok = new StringTokenizer(stringKey, "+");

		while (strtok.hasMoreElements()) {
			wb = (String) strtok.nextElement();
			wb = wb.trim();
			field = database.getField(wb);
			keyControl.addElement(field);
		} /* endwhile */

		while (reading > 0) {
			if (topNode == null) {
				lNode = new MNode(mfile, key_per_Node, key_length, keyType,
						Index_record, false);
			} else {
				MNode llNode = new MNode(mfile, key_per_Node, key_length,
						keyType, Index_record, false);
				lNode.set_prev(llNode);
				lNode = (MNode) llNode;
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
				topNode = (MNode) lNode.clone();
		}// /* endwhile */

		try {
			dosname = new String(tagDesc.tagname, DBF.encodedType);
		} catch (UnsupportedEncodingException UEE) {
			dosname = new String(tagDesc.tagname);
		}

	}

	public MDX(String iname, String NDXString, DBF iDBF, MDXFile ifile,
			TagDescriptor inTagDesc, short pos, boolean unique)
			throws xBaseJException, IOException

	{
		dosname = iname;
		database = iDBF;
		mfile = ifile;
		nfile = ifile.getRandomAccessFile();
		int i = iname.length();
		int j;

		unique_key = (byte) (unique ? 64 : 0);

		if ((i < 1) || (i > 10))
			throw new xBaseJException("Invalid tag name " + iname
					+ " name length incorrect");

		if (!Character.isLetter(iname.charAt(0)))
			throw new xBaseJException("Invalid tag name " + iname
					+ " first character not alphabetic");

		iname = iname.toUpperCase();

		for (j = 1; j < i; j++) {
			if (Character.isLetter(iname.charAt(j))
					|| (Character.isDigit(iname.charAt(j)) || (iname.charAt(j) == '_')))
				;
			else
				throw new xBaseJException("Invalid tag name " + iname
						+ " invalid character at position " + (j + 1));
		}

		StringTokenizer strtok = new StringTokenizer(NDXString, "+");

		String fname;
		Field ffld;
		char type;

		int keylen = 0;
		keyType = ' ';

		do {
			fname = (String) strtok.nextElement();
			ffld = iDBF.getField(fname);
			type = ffld.getType();
			if (type == 'M')
				throw new xBaseJException("Can't make memo field part of a key");
			if (type == 'L')
				throw new xBaseJException(
						"Can't make logical field part of a key");
			if (type == 'F')
				throw new xBaseJException(
						"Can't make float field part of a key");
			if (keyType == ' ') {
				keyType = type;
			} else {
				if (keyType != type) {
					keyType = 'C';
				} /* endif */
			} /* endif */
			keylen += ffld.getLength();
			keyControl.addElement(ffld);
		} while (strtok.hasMoreElements()); /* endwhile */

		if (keyType == 'D')
			keylen = 8;
		if (keyType == 'N')
			keylen = 12;

		int len = (((keylen - 1) / 4) + 1) * 4;
		if (len < 1)
			throw new xBaseJException("Key length too short");

		if (len > 100)
			throw new xBaseJException("Key length too long");

		tagDesc = inTagDesc;
		tagDesc.setKeyType(keyType);
		tagHead = new TagHeader(mfile, (short) tagDesc.indheaderpage,
				(short) len, keyType, unique);

		if (keyType == 'N')
			keyType = 'F'; // now flip to xbase definition of.
		if (keyType == 'D')
			keyType = 'N';

		key_length = tagHead.key_length;
		key_per_Node = tagHead.key_per_Node;

		byte kd[];
		try {
			kd = NDXString.toUpperCase().substring(0, NDXString.length())
					.getBytes(DBF.encodedType);
		} catch (UnsupportedEncodingException UEE) {
			kd = NDXString.toUpperCase().substring(0, NDXString.length())
					.getBytes();
		}
		for (int x = 0; x < kd.length; x++)
			tagHead.key_definition[x] = kd[x];

		if (database.getRecordCount() > 0)
			reIndex();
		else {
			tagDesc.write();
			tagHead.write();
		}

	}

	public void reIndex() throws xBaseJException, IOException {
		int i;
		int reccount = database.getRecordCount();
		NodeKey lastkey;
		BinaryTree topTree = null;
		if (database.getRecordCount() > 0) {
			database.gotoRecord(1);
			top_Node = 0;

			for (i = 1; i <= reccount; i++) {
				lastkey = build_key();
				if (topTree == null)
					topTree = new BinaryTree(lastkey, i, topTree);
				else
					new BinaryTree(lastkey, i, topTree);

				if (i < reccount)
					database.read();
			}
		}

		topNode = null;
		if (database.getRecordCount() > 0)
			reIndexWork(topTree.getLeast(), 0, topTree);
		tagHead.top_Node = top_Node;
		tagHead.write();
		tagDesc.write();
		anchor_write();
	}

	private int reIndexWork(BinaryTree bt, int level, BinaryTree topTree)
			throws IOException, xBaseJException {

		int prev_page = 0;
		BinaryTree tree2 = null;
		int pos = 0;
		if (level > 0)
			prev_page = top_Node;
		top_Node = (short) mfile.getAnchor().get_nextavailable();
		workNode = new MNode(mfile, key_per_Node, key_length, keyType,
				top_Node, level > 0);
		workNode.set_prev_page(prev_page);
		mfile.getAnchor().update_nextavailable();
		tagHead.pagesused += mfile.getAnchor().get_blocksize();

		workNode.set_pos(0);
		NodeKey lastKey = null;
		btLoop: while (true) {
			if (pos == key_per_Node || bt == null) {
				if ((tree2 == null && level > 0 && pos == 1) || pos == 0) {
					top_Node--;
					mfile.getAnchor().reset_nextavailable();
					topNode = workNode; // just in case its not set
					for (int i = pos; i < key_per_Node; i++) {
						workNode.set_pos(i);
						workNode.set_key_value(lastKey);
					}
					workNode.write();
					break btLoop;
				}
				if (bt != null || tree2 != null) {
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
				if (bt == null) {
					topNode = workNode;
					break btLoop; // we're all done
				}
				top_Node = (short) mfile.getAnchor().get_nextavailable();
				workNode = new MNode(mfile, key_per_Node, key_length, keyType,
						top_Node, level > 0);
				mfile.getAnchor().update_nextavailable();
				tagHead.pagesused += mfile.getAnchor().get_blocksize();
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

		topTree = null;
		System.gc();

		return reIndexWork(tree2.getLeast(), ++level, tree2);

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
		record = find_entry(findKey, (MNode) topNode, rec);
		return record;
	}

	public int find_entry(NodeKey findKey, MNode aNode, int findrec)
			throws xBaseJException, IOException {
		int rec, leaf, until;
		int stat = 0;
		MNode Node_2;
		foundExact = false;
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

		for (aNode.set_pos(0); aNode.get_pos() < until; aNode.pos_up()) {
			leaf = aNode.get_lower_level();
			rec = aNode.get_key_record_number();
			if (aNode.get_pos() < (aNode.get_keys_in_this_Node())) { /*
																	 * leafs
																	 * make us
																	 * do this
																	 */
				stat = findKey.compareKey(aNode.get_key_value());
				if (stat > 0)
					continue;
			}
			if (leaf > 0) { /* still dealing with Nodes */
				if (aNode.get_next() == null) {
					Node_2 = new MNode(mfile, key_per_Node, key_length,
							keyType, leaf, true);
					aNode.set_next(Node_2);
					Node_2.set_prev(aNode);
				} else
					Node_2 = (MNode) aNode.get_next();
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

			/* stat is zero - key matches the current key */

			foundExact = true;

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

	public int get_next_key() throws xBaseJException, IOException {
		return get_next_key((MNode) workNode);
	}

	private int get_next_key(MNode aNode) throws xBaseJException, IOException {

		int rec, until, leaf;

		if (aNode == null)
			return -1;

		leaf = aNode.get_lower_level();

		if (aNode.branch) /*
						 * leaf pointers usually have one more pointer than
						 * shown
						 */
			until = aNode.get_keys_in_this_Node() + 1;
		else
			until = aNode.get_keys_in_this_Node();

		aNode.pos_up();

		if (aNode.get_pos() >= until) {
			MNode rNode;
			rNode = (MNode) aNode.get_prev();
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
		return get_prev_key((MNode) workNode);
	}

	private int get_prev_key(MNode aNode) throws xBaseJException, IOException {
		int rec, until, leaf;

		if (aNode == null)
			return -1;

		if (aNode.get_pos() < 0)
			return -1;

		leaf = aNode.get_lower_level();
		until = 0;

		if (aNode.get_pos() > -1)
			aNode.pos_down();

		if (aNode.get_pos() < 0) {
			rec = get_prev_key((MNode) aNode.get_prev());
			if (rec == -1) {
				return -1;
			}
			aNode.set_record_number(rec);
			aNode.read();
			aNode.set_pos(aNode.get_keys_in_this_Node() + until); // must be
			// leaf node
			aNode.pos_down(); // offset is at zero not 1
		} /* endif */

		leaf = aNode.get_lower_level();
		workNode = aNode;
		if (leaf > 0) {
			return (leaf);
		}
		return aNode.get_key_record_number();
	}

	private void anchor_write() throws IOException {
		mfile.getAnchor().write();
	}

	public int add_entry(NodeKey NDXkey, int recno) throws xBaseJException,
			IOException {

		if (topNode != null) {
			find_entry(NDXkey, findAnyKey);
		}

		set_active_key(NDXkey);
		return update_entry((MNode) workNode, NDXkey, recno, 0);

	}

	private int update_entry(MNode aNode, NodeKey NDXkey, int recno,
			int oldrecno) throws IOException, xBaseJException {

		int savepos;
		if (topNode == null || topNode.get_record_number() == 0) { /*
																	 * we don't
																	 * have any
																	 * Index
																	 * area yet
																	 * so we
																	 * must be
																	 * adding
																	 * the first
																	 * record
																	 */
			if (topNode == null)
				topNode = new MNode(mfile, key_per_Node, key_length, keyType,
						mfile.getAnchor().get_nextavailable(), false);
			workNode = topNode;
			topNode.set_next(null);
			topNode.set_prev(null);
			topNode.set_pos(0);
			topNode.set_key_record_number(recno);

			topNode.set_key_value(NDXkey);
			topNode.set_keys_in_this_Node(1);

			tagHead.top_Node = mfile.getAnchor().get_nextavailable();

			topNode.set_record_number((int) tagHead.top_Node);
			topNode.set_lower_level(0);
			topNode.write();

			tagHead.top_Node = mfile.getAnchor().get_nextavailable();
			tagHead.pagesused += mfile.getAnchor().get_blocksize();
			tagHead.write();

			mfile.getAnchor().update_nextavailable();

			return 0;
		}

		if (aNode == null) { /* work to split the top Node */
			MNode newNode = new MNode(mfile, key_per_Node, key_length, keyType,
					mfile.getAnchor().get_nextavailable(), true);
			newNode.set_next(null);
			newNode.set_prev(null);
			topNode = newNode;

			newNode.set_pos(0);
			newNode.set_key_record_number((int) recno);
			newNode.set_key_value(NDXkey);
			newNode.set_pos(1);
			newNode.set_key_record_number((int) oldrecno);
			newNode.set_pos(0);

			newNode.set_keys_in_this_Node(1);

			tagHead.top_Node = mfile.getAnchor().get_nextavailable();

			newNode.set_record_number((int) tagHead.top_Node);
			newNode.write();

			tagHead.pagesused += mfile.getAnchor().get_blocksize();
			tagHead.write();

			mfile.getAnchor().update_nextavailable();

			return 0;
		}

		savepos = aNode.get_pos();
		if (savepos < (aNode.get_keys_in_this_Node())) {
			int ptr, recn, i;
			NodeKey buf;
			i = aNode.get_keys_in_this_Node();
			aNode.set_pos(i);
			ptr = aNode.get_lower_level();
			aNode.pos_up();
			aNode.set_lower_level(ptr);
			/* then move all the other subNodes */
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
			aNode.set_lower_level(recno);
			aNode.set_key_record_number(recno);
			aNode.set_key_value(NDXkey);
			if (oldrecno > 0) {
				aNode.pos_up();
				aNode.set_lower_level(oldrecno);
				aNode.pos_down();
			}
		} else {
			aNode.set_pos(aNode.get_keys_in_this_Node());
			aNode.set_lower_level(recno);
			aNode.set_key_record_number(recno);
			aNode.set_key_value(NDXkey);
			if (oldrecno > 0) {
				aNode.pos_up();
				aNode.set_lower_level(oldrecno);
				aNode.pos_down();
			}
		}
		aNode.set_keys_in_this_Node(aNode.get_keys_in_this_Node() + 1);
		aNode.write();

		if (aNode.get_keys_in_this_Node() > key_per_Node)
			splitNode(aNode, savepos);

		return 0;
	}

	private void splitNode(MNode aNode, int savepos) throws xBaseJException,
			IOException {
		int i, j, k;
		int left, right;
		MNode bNode;

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
		bNode = new MNode(mfile, key_per_Node, key_length, keyType, 0, aNode
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
		// bNode.set_key_record_number(0);
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
			right = mfile.getAnchor().get_nextavailable();
			mfile.getAnchor().update_nextavailable();
			if (aNode.get_prev() != null) {
				bNode.set_pos(i - 1);
				update_entry((MNode) aNode.get_prev(), bNode.get_key_value(),
						left, right);
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
				update_entry(null, bNode.get_key_value(), left, right);
				aNode.set_prev(topNode);
				topNode.set_next(aNode);
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
			aNode.set_record_number(mfile.getAnchor().get_nextavailable()); // did a
			// split
			// renumber
			// Node
			mfile.getAnchor().update_nextavailable();
			if (aNode.get_next() != null)
				aNode.set_keys_in_this_Node(j - 1);
			else
				aNode.set_keys_in_this_Node(j);

			aNode.set_pos(j - 1);
			aNode.write();
			left = aNode.get_record_number();
			update_entry((MNode) aNode.get_prev(), aNode.get_key_value(), left,
					right);
			if (aNode.get_prev() == null) {
				aNode.set_prev(topNode);
				topNode.set_next(aNode);
			}
		}
		bNode = null;
	}

	public void del_entry(Node inNode) throws IOException, xBaseJException {

		MNode aNode;
		int pos, k;

		aNode = (MNode) inNode;

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

}
