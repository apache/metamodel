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
import java.nio.ByteBuffer;

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
public class LogicalField extends Field {

	private static final long serialVersionUID = 1L;
	public final static byte BYTETRUE = (byte) 'T';
	public final static byte BYTEFALSE = (byte) 'F';

	public Object clone() throws CloneNotSupportedException {
		LogicalField tField = (LogicalField) super.clone();
		tField.name = new String(name);
		tField.nength = 1;
		return tField;
	}

	public LogicalField() {
		super();
	}

	public LogicalField(String iName, ByteBuffer inBuffer)
			throws xBaseJException {
		super();
		super.setField(iName, 1, inBuffer);
		put('F');

	}

	/**
	 * public method for creating a LogicalField object. It is not associated
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

	public LogicalField(String iName) throws xBaseJException, IOException {
		super();
		super.setField(iName, 1, null);
	}

	/**
	 * return the character 'L' indicating a logical Field
	 */

	public char getType() {
		return 'L';
	}

	/**
	 *allows input of Y, y, T, t and 1 for true, N, n, F, f, and 0 for false
	 * 
	 * @throws xBaseJException
	 *             most likely a format exception
	 */

	public void put(String inValue) throws xBaseJException {

		String value = inValue.trim();

		if (DbaseUtils.dontTrimFields() == false)
			value = inValue;

		if (value.length() == 0)
			value = "F";

		if (value.length() != 1)
			throw new xBaseJException("Field length incorrect");

		put(value.charAt(0));

	}

	/**
	 *allows input of Y, y, T, t and 1 for true, N, n, F, f, and 0 for false
	 * 
	 * @throws xBaseJException
	 *             most likely a format exception
	 */
	public void put(char inValue) throws xBaseJException {
		switch (inValue) {
		case 'Y':
		case 'y':
		case 'T':
		case 't':
		case '1':
			buffer[0] = BYTETRUE;
			break;
		case 'N':
		case 'n':
		case 'F':
		case 'f':
		case '0':
			buffer[0] = BYTEFALSE;
			break;
		default:
			throw new xBaseJException("Invalid logical Field value");
		}
	}

	/**
	 * allows input true or false
	 */
	public void put(boolean inValue) {
		if (inValue)
			buffer[0] = BYTETRUE;
		else
			buffer[0] = BYTEFALSE;
	}

	/**
	 * returns T for true and F for false
	 */
	public char getChar() {
		return (char) buffer[0];
	}

	/**
	 * returns true or false
	 */
	public boolean getBoolean() {
		return ((buffer[0] == BYTETRUE));
	}

}
