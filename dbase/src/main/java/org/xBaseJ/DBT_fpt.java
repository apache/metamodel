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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;

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
public class DBT_fpt extends DBTFile {

	int blocks = 0;
	int memoBlockSize;

	public DBT_fpt(DBF iDBF, boolean readOnly) throws IOException,
			xBaseJException {
		super(iDBF, readOnly, DBF.FOXPRO_WITH_MEMO);
		nextBlock = file.readInt();
		// file.skipBytes(2);
		memoBlockSize = file.readInt();
	}

	public DBT_fpt(DBF iDBF, String name, boolean destroy) throws IOException,
			xBaseJException {
		super(iDBF, name, destroy, DBF.FOXPRO_WITH_MEMO);
		nextBlock = 8;
		file.writeInt(nextBlock);
		file.writeByte(0);
		file.writeByte(0);
		memoBlockSize = 64;
		file.writeShort(memoBlockSize);
		for (int i = 0; i < 504; i += 4)
			file.writeInt(0);
	}

	public void rename(String name) throws IOException {

		String tname = new String(name.substring(0, name.length() - 3) + "fpt");
		file.close();
		File nfile = new File(tname);
		nfile.delete();
		thefile.renameTo(nfile);
		thefile = nfile;
		file = new RandomAccessFile(tname, "rw");
	}

	public void setNextBlock() throws IOException {

	}

	public byte[] readBytes(byte[] input) throws IOException, xBaseJException {

		int i;
		for (i = 0; i < 10; i++) {
			if (input[i] >= BYTEZERO && input[i] <= '9')
				break;
			if (input[i] == BYTESPACE)
				input[i] = BYTEZERO;
		}

		String sPos = new String(input, 0, 10);

		for (i = 0; i < sPos.length(); i++)
			if (sPos.charAt(i) != BYTESPACE)
				break;
		if (i == sPos.length())
			return null;

		int lPos = Integer.parseInt(sPos.trim());

		if (lPos == 0)
			return null;
		long longpos = lPos;
		file.seek((longpos * memoBlockSize));

		int orisize;

		orisize = 0;

		file.skipBytes(4); /* [ 1985813 ] Bug in DBT_fpt.java */
		int size = file.readInt();

		orisize = size;

		byte work_buffer[] = new byte[orisize];
		file.read(work_buffer, 0, orisize);

		return work_buffer;

	}

	public byte[] write(String value, int originalSize, boolean write,
			byte originalPos[]) throws IOException, xBaseJException {
		try {
			return write(value.getBytes(DBF.encodedType), originalSize, write,
					originalPos);
		} catch (UnsupportedEncodingException UEE) {
			return write(value.getBytes(), originalSize, write, originalPos);
		}
	}

	public byte[] write(byte inBytes[], int originalSize, boolean write,
			byte originalPos[]) throws IOException, xBaseJException {

		int pos, startPos;
		int length;
		boolean madebigger = false;

		if (inBytes.length == 0) {
			byte breturn[] = { BYTESPACE, BYTESPACE, BYTESPACE, BYTESPACE,
					BYTESPACE, BYTESPACE, BYTESPACE, BYTESPACE, BYTESPACE,
					BYTESPACE };
			return breturn;
		}

		if ((originalSize == 0) && (inBytes.length > 0))
			madebigger = true;
		else if (((inBytes.length / memoBlockSize) + 1) > ((originalSize / memoBlockSize) + 1))
			madebigger = true;
		else
			madebigger = false;

		if (madebigger || write) {
			startPos = nextBlock;
			nextBlock += ((inBytes.length + 2) / memoBlockSize) + 1;

			file.seek(0);
			file.writeInt(nextBlock);
		} else {
			String sPos;
			sPos = new String(originalPos, 0, 10);
			startPos = Integer.parseInt(sPos.trim());

		} /* endif */

		length = inBytes.length;

		pos = (length / memoBlockSize) + 1;

		long longpos = startPos;
		file.seek((longpos * memoBlockSize));

		int inType = 1;
		file.writeInt(inType);
		file.writeInt(length);

		byte buffer[] = inBytes;
		file.write(buffer, 0, length);

		length = memoBlockSize - ((length) % memoBlockSize);

		if (length < memoBlockSize)
			while (length-- > 0)
				file.write(0);

		String returnString = new String(Long.toString(startPos));

		byte ten[] = new byte[10];

		for (pos = 0; pos < (10 - returnString.length()); pos++)
			ten[pos] = BYTEZERO;

		byte b[];
		b = returnString.getBytes();
		for (int x = 0; x < b.length; x++, pos++)
			ten[pos] = b[x];

		return ten;
	}

}
