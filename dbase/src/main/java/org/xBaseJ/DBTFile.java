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

package org.xBaseJ;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

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
public abstract class DBTFile {

	public final static byte BYTEZERO = (byte) '0';
	public final static byte BYTESPACE = (byte) ' ';

	protected RandomAccessFile file;
	protected boolean open;
	protected File thefile;
	protected int memoBlockSize = 512;
	protected int nextBlock;
	protected DBF database;
	protected String extension = "dbt";

	public void rename(String name) throws IOException {

		String tname = new String(name.substring(0, name.length() - 1) + "t");
		file.close();
		File nfile = new File(tname);
		nfile.delete();
		thefile.renameTo(nfile);
		thefile = nfile;
		file = new RandomAccessFile(tname, "rw");
	}

	public DBTFile(DBF iDBF, boolean readonly, int type) throws IOException,
			xBaseJException, IOException {
		database = iDBF;
		String name = iDBF.getName();

		String tname;
		String ext = DbaseUtils.getxBaseJProperty("memoFileExtension");
		if (ext.length() > 0) {
			tname = new String(name.substring(0, name.length() - 3) + ext);
			extension = ext;
		} else if (type == DBF.FOXPRO_WITH_MEMO) {
			extension = "fpt";
			tname = new String(name.substring(0, name.length() - 3) + extension);
			thefile = new File(tname);
			if (!thefile.exists() || !thefile.isFile()) {
				throw new xBaseJException("Can't find Memo Text file " + tname);
			} /* endif */
		} else {
			tname = new String(name.substring(0, name.length() - 3) + extension);

			thefile = new File(tname);
			if (!thefile.exists() || !thefile.isFile()) {
				String dtname = new String(name.substring(0, name.length() - 3)
						+ "fpt");
				thefile = new File(dtname);
				if (!thefile.exists() || !thefile.isFile()) {
					throw new xBaseJException("Can't find Memo Text file "
							+ dtname);
				} else {
					// type = DBF.FOXPRO_WITH_MEMO;
					tname = dtname;
				}

			}
		}/* endif */
		if (readonly)
			file = new RandomAccessFile(tname, "r");
		else
			file = new RandomAccessFile(tname, "rw");
		setNextBlock();

	}

	public DBTFile(DBF iDBF, String name, boolean destroy, int type)
			throws IOException, xBaseJException {

		database = iDBF;

		String tname;
		String ext = DbaseUtils.getxBaseJProperty("memoFileExtension");
		if (ext.length() > 0) {
			extension = ext;
		} else if (type == DBF.FOXPRO_WITH_MEMO) // foxpro
			extension = "fpt";

		tname = new String(name.substring(0, name.length() - 3) + extension);

		thefile = new File(tname);

		if (destroy == false)
			if (thefile.exists())
				throw new xBaseJException(
						"Memeo Text File exists, can't destroy");

		if (destroy)
			if (thefile.exists())
				if (thefile.delete() == false)
					throw new xBaseJException("Can't delete old Memo Text file");

		FileOutputStream tFOS = new FileOutputStream(thefile);
		tFOS.close();

		file = new RandomAccessFile(thefile, "rw");

		setNextBlock();

	}

	public abstract void setNextBlock() throws IOException;

	public abstract byte[] readBytes(byte[] input) throws IOException,
			xBaseJException;

	public abstract byte[] write(String value, int originalSize, boolean write,
			byte originalPos[]) throws IOException, xBaseJException;

	public void seek(int pos) throws IOException {
		long lpos = pos;
		file.seek(lpos);
	}

	public void close() throws IOException {
		open = false;
		file.close();

	}
}
