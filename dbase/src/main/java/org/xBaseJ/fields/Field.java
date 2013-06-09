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

package org.xBaseJ.fields;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.xBaseJ.DBF;
import org.xBaseJ.DbaseUtils;
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
public abstract class Field implements Cloneable, Externalizable {

	protected String name;
	protected int nength = 0;
	protected byte[] buffer;
	protected boolean deleted;
	protected ByteBuffer bytebuffer;
	protected long myoffset;

	/**
	 * used by externalize methods
	 * 
	 * @param in
	 *            ObjectInput stream
	 * @throws IOException
	 *             - most likely class changed since written
	 * @throws ClassNotFoundException
	 *             - only when dummy constructro not found
	 */

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		name = in.readUTF();
		nength = in.readInt();
		in.readFully(buffer);
		deleted = in.readBoolean();
	}

	/**
	 * used by externalize methods
	 * 
	 * @param out
	 *            ObjectOutput stream
	 * @throws IOException
	 *             Java.io error
	 */

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(nength);
		out.write(buffer);
		out.writeBoolean(deleted);
	}

	public Object clone() throws CloneNotSupportedException {
		Field tField = (Field) super.clone();
		tField.name = new String(name);
		tField.nength = nength;
		// tField.buffer = buffer;
		// tField.bytebuffer = bytebuffer;
		return tField;
	}

	public static String otherValidCharacters = null;

	private void validateName(String iName) throws xBaseJException {

		if (otherValidCharacters == null) {
			try {
				otherValidCharacters = DbaseUtils
						.getxBaseJProperty("otherValidCharactersInFieldNames");
			} catch (IOException e) {
				otherValidCharacters = "";
			}
			if (otherValidCharacters == null)
				otherValidCharacters = "";
		}
		if (iName == null)
			throw new xBaseJException("Missing field name");
		if (iName.length() == 0)
			throw new xBaseJException("Missing field name");
		if (iName.length() > 10)
			throw new xBaseJException("Invalid field name " + iName);

		for (int i = 0; i < iName.length(); i++) {
			if (Character.isLetter(iName.charAt(i)))
				continue;
			if (Character.isDigit(iName.charAt(i)))
				continue;
			if (iName.charAt(i) == '_')
				continue;
			if (otherValidCharacters.indexOf(iName.charAt(i)) > -1)
				continue;

			throw new xBaseJException("Invalid field name " + iName
					+ ", character invalid at " + i);
		}

	}

	/**
	 * creates a Field object. not useful for the abstract Field class
	 * 
	 * @see CharField
	 * @see DateField
	 * @see LogicalField
	 * @see MemoField
	 * @see NumField
	 */
	public Field() {
		int tlength;

		if (nength == 0)
			tlength = 1;
		else
			tlength = nength;

		buffer = new byte[tlength];
		buffer[0] = (byte) ' ';
	}

	public void setField(String iName, int iLength, ByteBuffer inbuffer)
			throws xBaseJException {

		name = iName.trim();
		validateName(name);
		nength = iLength;
		setBuffer(inbuffer);
		// buffer = new byte[Length];

	}

	public void setBuffer(ByteBuffer inBuffer) {
		bytebuffer = inBuffer;
		setBufferSpace();
	}

	public void setBufferSpace() {
		buffer = new byte[nength];
	}

	/**
	 * @return String contianing the field name
	 */

	public String getName() {
		return name;
	}

	/**
	 * @return int - the field length
	 */

	public int getLength() {
		return nength;
	}

	/**
	 * @return char field type
	 */
	public abstract char getType();

	/**
	 * @return int - the number of decimal positions for numeric fields, zero
	 *         returned otherwise
	 */
	public int getDecimalPositionCount() {
		return 0;
	}

	public void read() throws IOException, xBaseJException {
		bytebuffer.get(buffer);
	}

	/**
	 * @return String field contents after any type of read.
	 */
	public String get() {
		int k;
		for (k = 0; k < nength && buffer[k] != 0; k++)
			;
		if (k == 0) // no data
			return "";

		String s;
		try {
			s = new String(buffer, 0, k, DBF.encodedType);
		} catch (UnsupportedEncodingException UEE) {
			s = new String(buffer, 0, k);
		}
		return s;
	}

	/**
	 * returns the original byte array as stored in the file.
	 * 
	 * @return byte[] - may return a null if not set
	 */
	public byte[] getBytes() {
		return buffer;
	}

	public void write() throws IOException, xBaseJException {
		bytebuffer.put(buffer);
	}

	public void update() throws IOException, xBaseJException {
		bytebuffer.put(buffer);
	}

	/**
	 * set field contents, no database updates until a DBF update or write is
	 * issued
	 * 
	 * @param inValue
	 *            value to set
	 * @throws xBaseJException
	 *             value length too long
	 */

	public void put(String inValue) throws xBaseJException {
		byte b[];
		int i;

		if (inValue.length() > nength)
			throw new xBaseJException("Field length too long");

		i = Math.min(inValue.length(), nength);

		try {
			b = inValue.getBytes(DBF.encodedType);
		} catch (UnsupportedEncodingException UEE) {
			b = inValue.getBytes();
		}

		for (i = 0; i < b.length; i++)
			buffer[i] = b[i];

		byte fill;
		if (DbaseUtils.fieldFilledWithSpaces())
			fill = (byte) ' ';
		else
			fill = 0;

		for (i = inValue.length(); i < nength; i++)
			buffer[i] = fill;

	}

	/**
	 * set field contents with binary data, no database updates until a DBF
	 * update or write is issued if inValue is too short buffer is filled with
	 * binary zeros.
	 * 
	 * @param inValue
	 *            byte array
	 * @throws xBaseJException
	 *             value length too long
	 */

	public void put(byte inValue[]) throws xBaseJException {
		int i;

		if (inValue.length > nength)
			throw new xBaseJException("Field length too long");

		for (i = 0; i < inValue.length; i++)
			buffer[i] = inValue[i];

		for (; i < nength; i++)
			if (DbaseUtils.fieldFilledWithSpaces())
				buffer[i] = ' ';
			else
				buffer[i] = 0;

	}

}
