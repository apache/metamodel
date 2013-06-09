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
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;

import org.xBaseJ.DBF;
import org.xBaseJ.DbaseUtils;

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
public class TagDescriptor {

	static final short BLOCKLENGTH = 512;
	static final short descriptorLength = 32;
	int indheaderpage; /* page number of Index header */
	byte tagname[] = new byte[11]; /* the tag name, in caps, null-filled */
	byte Fieldflag; /* 10 if tag is a Field, else 0 */
	byte forwardtag = 0;
	byte forwardtag_greater = 0;
	byte backwardtag = 0;
	byte useless = 2;
	byte keytype; /* C, D, or N for key type */
	byte rsrvd[] = new byte[11];

	String name;
	RandomAccessFile nfile;
	long pos;

	public TagDescriptor(RandomAccessFile ifile, short ipos) throws IOException {
		nfile = ifile;
		pos = (long) ((BLOCKLENGTH) + (ipos * descriptorLength));
		read();
		try {
			name = new String(tagname, DBF.encodedType).trim();
		} catch (UnsupportedEncodingException UEE) {
			name = new String(tagname).trim();
		}
	}

	public TagDescriptor(MDXFile ifile, short ipos, String iName)
			throws IOException {

		nfile = ifile.getRandomAccessFile();
		pos = (long) ((BLOCKLENGTH) + (ipos * descriptorLength));
		name = iName;
		byte tname[];
		try {
			tname = iName.getBytes(DBF.encodedType);
		} catch (UnsupportedEncodingException UEE) {
			tname = iName.getBytes();
		}
		for (int x = 0; x < tname.length; x++)
			tagname[x] = tname[x];

		indheaderpage = ifile.getAnchor().get_nextavailable();
		keytype = (byte) ' ';
		if (ipos > 1)
			backwardtag = (byte) (ipos - 1);

		Fieldflag = 16;

		write();

	}

	public void reset(RandomAccessFile ifile) {
		nfile = ifile;
	}

	public void setKeyType(char type) throws IOException {
		keytype = (byte) type;
		write();

	}

	public void resetPos(short ipos) {
		pos = (long) ((BLOCKLENGTH) + (ipos * descriptorLength));

	}

	void read() throws IOException {
		nfile.seek(pos);
		indheaderpage = nfile.readInt();
		nfile.read(tagname);
		try {
			name = new String(tagname, DBF.encodedType);
		} catch (UnsupportedEncodingException UEE) {
			name = new String(tagname);
		}
		Fieldflag = nfile.readByte();
		forwardtag = nfile.readByte();
		forwardtag_greater = nfile.readByte();
		backwardtag = nfile.readByte();
		useless = nfile.readByte();
		keytype = nfile.readByte();
		nfile.read(rsrvd);
		redo_numbers();
	}

	void write() throws IOException {
		redo_numbers();
		nfile.seek(pos);
		nfile.writeInt(indheaderpage);
		nfile.write(tagname);
		nfile.writeByte(Fieldflag);
		nfile.write(forwardtag);
		nfile.write(forwardtag_greater);
		nfile.write(backwardtag);
		nfile.writeByte(useless);
		nfile.writeByte(keytype);
		nfile.write(rsrvd);
		redo_numbers();

	}

	void updateForwardTag(short pos) throws IOException {
		forwardtag = (byte) pos;
		write();
	}

	void redo_numbers() {

		indheaderpage = DbaseUtils.x86(indheaderpage);
	}

	public String getName() {
		return name;
	}

}
