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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.xBaseJ.DBF;
import org.xBaseJ.DBTFile;
import org.xBaseJ.DBT_fpt;
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
public class PictureField extends Field {

	private static final long serialVersionUID = 1L;

	private DBT_fpt dbtobj;
	private int originalSize;
	private String value;
	private byte[] byteValue;

	public PictureField() {
		super();
	}

	public void setDBTObj(DBTFile indbtobj) {
		dbtobj = (DBT_fpt) indbtobj;

	}

	public Object clone() throws CloneNotSupportedException {
		try {
			PictureField tField = new PictureField(name, null, null);
			return tField;
		} catch (xBaseJException e) {
			return null;
		} catch (IOException e) {
			return null;
		}
	}

	public PictureField(String Name, ByteBuffer inBuffer, DBTFile indbtobj)
			throws xBaseJException, IOException {
		super();
		super.setField(Name, 10, inBuffer);
		dbtobj = (DBT_fpt) indbtobj;
		value = new String("");
	}

	/**
	 * public method for creating a picture field object. It is not associated
	 * with a database but can be when used with some DBF methods.
	 * 
	 * @param iName
	 *            the name of the field
	 * @throws xBaseJException
	 *             exception caused in calling methods
	 * @throws IOException
	 *             can not occur but defined for calling methods
	 * @see Field
	 * 
	 */

	public PictureField(String iName) throws xBaseJException, IOException {
		super();
		super.setField(iName, 10, null);
		dbtobj = null;
		originalSize = 0;
		buffer = new byte[10];
		for (int i = 0; i < 10; i++)
			buffer[i] = DBTFile.BYTEZERO;
		value = new String("");
	}

	/**
	 * return the character 'P' indicating a picture field
	 */
	public char getType() {
		return 'P';
	}

	/**
	 * return the contents of the picture Field, variant of the field.get method
	 */
	public String get() {
		if (byteValue == null)
			return "";
		try {
			return new String(byteValue, DBF.encodedType);
		} catch (UnsupportedEncodingException UEE) {
			return new String(byteValue);
		}
	}

	/**
	 * return the contents of the picture Field via its original byte array
	 * 
	 * @return byte[] - if not set a null is returned.
	 */
	public byte[] getBytes() {
		return byteValue;
	}

	public void read() throws IOException, xBaseJException {
		super.read();
		byteValue = dbtobj.readBytes(super.buffer);
		if (byteValue == null)
			originalSize = 0;
		else
			originalSize = value.length();
	}

	/**
	 * sets the contents of the picture Field, variant of the field.put method
	 * data not written into DBF until an update or write is issued.
	 * 
	 * @param invalue
	 *            value to set Field to.
	 */
	public void put(String invalue) throws xBaseJException {
		throw new xBaseJException("use put(Bytes[])");
	}

	/**
	 * sets the contents of the picture Field, variant of the field.put method
	 * data not written into DBF until an update or write is issued.
	 * 
	 * @param inBytes
	 *            value to set Field to.
	 */
	public void put(byte inBytes[]) throws xBaseJException {
		byteValue = inBytes;
	}

	public void write() throws IOException, xBaseJException {
		super.buffer = dbtobj
				.write(byteValue, originalSize, true, super.buffer);
		super.write();
	}

	public void update() throws IOException, xBaseJException {
		super.buffer = dbtobj.write(byteValue, originalSize, false,
				super.buffer);
		super.write();
	}

}
