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
public class CharField extends Field {

	private static final long serialVersionUID = 1L;

	public Object clone() throws CloneNotSupportedException {
		CharField tField = (CharField) super.clone();
		tField.name = new String(name);
		tField.nength = nength;
		return tField;
	}

	public CharField() {
		super();
	}

	public CharField(String iName, int iLength, ByteBuffer inBuffer)
			throws xBaseJException, IOException {
		super();
		super.setField(iName, iLength, inBuffer);
		put("");
	}

	/**
	 * public method for creating a CharacterField object. It is not associated
	 * with a database but can be when used with some DBF methods.
	 * 
	 * @param iName
	 *            the name of the field
	 * @param iLength
	 *            length of Field, range 1 to 254 bytes
	 * @throws xBaseJException
	 *             invalid length
	 * @throws IOException
	 *             can not occur but defined for calling methods
	 * @see Field
	 * 
	 */
	public CharField(String iName, int iLength) throws xBaseJException,
			IOException {
		super();
		super.setField(iName, iLength, null);
	}

	/**
	 * return the character 'C' indicating a character Field
	 */

	public char getType() {
		return 'C';
	}

}
