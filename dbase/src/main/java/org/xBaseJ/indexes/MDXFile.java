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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.xBaseJ.DBF;
import org.xBaseJ.xBaseJException;

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
public class MDXFile {

	private File file;
	private RandomAccessFile randomAccessFile;
	private String name;
	private MDXAnchor anchor;
	private TagDescriptor tags[];
	private MDX[] MDXes;
	private final short maxTags = 47;
	private DBF database;

	public MDXFile(String Name, DBF inDBF, char readonly) throws IOException,
			xBaseJException {

		short i;
		database = inDBF;

		// MDXFileData ; /* set in BeginInitializer */ ????????
		name = Name.substring(0, Name.lastIndexOf('.')) + ".mdx";

		file = new File(name);

		if (!file.exists())
			throw new xBaseJException("Missing mdx file:" + name);

		if (readonly == 'r')
			randomAccessFile = new RandomAccessFile(file, "r");
		else
			randomAccessFile = new RandomAccessFile(file, "rw");

		anchor = new MDXAnchor(randomAccessFile);
		anchor.read();
		tags = new TagDescriptor[maxTags];
		MDXes = new MDX[maxTags];
		for (i = 0; i < anchor.getIndexes(); i++) {
			tags[i] = new TagDescriptor(randomAccessFile, (short) (i + 1));
			MDXes[i] = new MDX(this, inDBF, i);
		}
		for (; i < maxTags; i++) {
			MDXes[i] = null;
			tags[i] = null;
		}

	}

	public MDXFile(String Name, DBF inDBF, boolean destroy) throws IOException {

		int i;
		database = inDBF;

		// MDXFileData ; /* set in BeginInitializer */ ????????

		name = Name.substring(0, Name.lastIndexOf('.')) + ".mdx";

		file = new File(name);

		FileOutputStream tFOS = new FileOutputStream(file);
		tFOS.close();

		randomAccessFile = new RandomAccessFile(file, "rw");
		anchor = new MDXAnchor(randomAccessFile);
		anchor.set(Name.substring(0, Name.lastIndexOf('.')));

		anchor.write();

		byte wb[] = new byte[32];

		for (i = 0; i < 32; i++)
			wb[i] = 0;
		randomAccessFile.seek(512);
		randomAccessFile.write(wb);

		tags = new TagDescriptor[maxTags];
		MDXes = new MDX[maxTags];
	}

	public void close() throws IOException {
		randomAccessFile.close();
	}

	public MDX getMDX(String Name) throws xBaseJException {
		int i;
		for (i = 0; i < anchor.getIndexes(); i++) {
			if (tags[i].name.equalsIgnoreCase(Name))
				return MDXes[i];
		}

		throw new xBaseJException("Unknown tag named " + Name);
	}

	TagDescriptor getTagDescriptor(int i) {
		return tags[i];
	}

	TagDescriptor getTagDescriptor(String Name) throws xBaseJException {
		int i;
		for (i = 0; i < anchor.getIndexes(); i++) {
			if (tags[i].name.equalsIgnoreCase(Name))
				return tags[i];
		}

		throw new xBaseJException("Unknown tag named " + Name);
	}

	public MDX createTag(String Name, String Index, boolean unique)
			throws IOException, xBaseJException {

		Name = Name.toUpperCase();
		if (anchor.getIndexes() >= maxTags)
			throw new xBaseJException("Can't create another tag. Maximum of "
					+ maxTags + " reached");

		try {
			getTagDescriptor(Name);
			throw new xBaseJException("Tag name already in use");
		} catch (xBaseJException e) {
			if (!e.getMessage().startsWith("Unknown tag named"))
				throw e;
		}

		short i = (short) (anchor.getIndexes() + 1);
		tags[i - 1] = new TagDescriptor(this, i, Name);
		MDX newMDX = new MDX(Name, Index, database, this, tags[i - 1], i,
				unique);
		anchor.setIndexes(i);
		anchor.write();
		MDXes[i - 1] = newMDX;

		if (i > 1)
			tags[i - 2].updateForwardTag(i);

		return newMDX;

	}

	short get_tag_count() {
		return anchor.getIndexes();
	}

	void set_blockbytes(short bytes) {
		anchor.blockbytes = bytes;

	}

	void drop_tag_count() throws IOException {
		anchor.addOneToIndexes();
		anchor.write();
	}

	void write_create_header() throws IOException {
		byte wb[] = new byte[32];

		for (int i = 0; i < 32; i++)
			wb[i] = 0;

		randomAccessFile.seek(512);

		randomAccessFile.write(wb);

	}

	public void reIndex() throws IOException, xBaseJException {
		short oldIndexCount = anchor.getIndexes();
		short i;
		randomAccessFile.close();
		file.delete();
		randomAccessFile = new RandomAccessFile(file, "rw");
		anchor.reset(randomAccessFile);
		anchor.write();
		for (i = 0; i < oldIndexCount; i++) {
			MDXes[i].tagDesc.indheaderpage = anchor.get_nextavailable();
			MDXes[i].tagDesc.reset(randomAccessFile);
			MDXes[i].tagDesc.write();
			MDXes[i].tagHead.reset(randomAccessFile);
			MDXes[i].tagHead.setPos((short) anchor.get_nextavailable());
			MDXes[i].tagHead.write();
			if (i > 1)
				tags[i - 2].updateForwardTag(i);
			anchor.update_nextavailable();
		}
		anchor.setIndexes(oldIndexCount);
		anchor.write();

	}

	public MDXAnchor getAnchor() {
		return anchor;
	}

	public RandomAccessFile getRandomAccessFile() {
		return randomAccessFile;
	}

	public MDX[] getMDXes() {
		return MDXes;
	}

	public MDX getMDX(int i) {
		return MDXes[i];
	}
}
