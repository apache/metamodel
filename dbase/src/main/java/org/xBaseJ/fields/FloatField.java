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
import java.text.DecimalFormatSymbols;

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
public class FloatField extends NumField {

	private static final long serialVersionUID = 1L;
	private byte decPosition = 0;

	private static char decimalSeparator = new DecimalFormatSymbols()
			.getDecimalSeparator();

	public FloatField() {
		super();
	}

	public FloatField(String iName, int iLength, int DecPoint,
			ByteBuffer inBuffer) throws xBaseJException {
		super(iName, iLength, DecPoint, inBuffer);
		decPosition = (byte) DecPoint;
	}

	/**
	 * public method for creating a numeric field object. It is not associated
	 * with a database but can be when used with some DBF methods.
	 * 
	 * @param iName
	 *            the name of the field
	 * @param iLength
	 *            the length of Field. range is 1 to 19 bytes
	 * @param DecPoint
	 *            the number of decimal positions range from 2 to 17 bytes.
	 *            Relative to Length.
	 * @throws xBaseJException
	 *             bad name, length or decimal positions specified
	 * @throws IOException
	 *             can not occur but defined for calling methods
	 * @see Field
	 * 
	 */

	public FloatField(String iName, int iLength, int DecPoint)
			throws xBaseJException, IOException {
		super(iName, iLength, DecPoint);
		decPosition = (byte) DecPoint;
	}

	/**
	 * return the character 'F' indicating a float field
	 */
	public char getType() {
		return 'F';
	}

	/**
	 * sets the field contents.
	 * 
	 * @throws xBaseJException
	 *             most likely a format exception
	 * @param inValue
	 *            String
	 */
	public void put(String inValue) throws xBaseJException {

		boolean signOn = false;
		if (inValue.trim().length() == 0) {
			super.put("");
			return;
		}

		int i;

		for (i = 0; i < inValue.length(); i++) {
			if (inValue.charAt(i) == '-')
				signOn = true;
			if (Character.isDigit(inValue.charAt(i))
					|| inValue.charAt(i) == decimalSeparator)
				break;
		}

		if (i == inValue.length()) {
			super.put(" ");
			return;
		}

		int start = i;

		for (; i < inValue.length(); i++) {
			if (!Character.isDigit(inValue.charAt(i))
					&& inValue.charAt(i) != decimalSeparator)
				break;
		}

		String workstring = inValue.substring(start, i);

		char charray[] = new char[nength];

		Double inDouble = new Double(workstring);
		double duble = inDouble.doubleValue();

		for (i = 0; i < decPosition; i++) {
			duble *= 10;
		}

		inDouble = new Double(duble);
		long longv = inDouble.longValue();

		if (longv < 0) {
			longv *= -1;
			signOn = true;
		}

		long longleft;
		int whatsleft;

		int realdp = nength - decPosition;
		String numstring = new String("0123456789");

		for (i = nength; i > 0; i--) {
			if ((realdp == i) && (decPosition > 0)) {
				charray[i - 1] = '.';
				continue;
			}

			longleft = longv % 10;
			whatsleft = (int) longleft;
			if (whatsleft < 0)
				whatsleft *= -1;

			charray[i - 1] = numstring.charAt(whatsleft);
			longv /= 10;
		}

		if (signOn)
			charray[0] = '-';
		for (i = 0; i < charray.length - 1; i++) {
			// if (signOn && i == 0)
			// continue;
			if (charray[i] != '0')
				break;
			charray[i] = ' ';
		}
		super.put(new String(charray).replace(decimalSeparator, '.'));
	}

	/**
	 * sets the field contents.
	 * 
	 * @param inValue
	 *            long
	 * @throws xBaseJException
	 *             most likely a format exception
	 */
	public void put(long inValue) throws xBaseJException {
		put(String.valueOf(inValue));
	}

	/**
	 * sets the field contents.
	 * 
	 * @param inValue
	 *            int
	 * @throws xBaseJException
	 *             most likely a format exception
	 */
	public void put(int inValue) throws xBaseJException {
		put(String.valueOf(inValue));
	}

	/**
	 * sets the field contents.
	 * 
	 * @param inValue
	 *            float
	 * @throws xBaseJException
	 *             most likely a format exception
	 */
	public void put(float inValue) throws xBaseJException {
		put(String.valueOf(inValue));
	}

	/**
	 * sets the field contents.
	 * 
	 * @param inValue
	 *            double
	 * @throws xBaseJException
	 *             most likely a format exception
	 */
	public void put(double inValue) throws xBaseJException {
		double d = inValue;
		double d10 = Math.pow(10, nength - decPosition);
		d %= d10;
		d10 = Math.pow(.1, decPosition + 1);
		if (d > 0)
			d += d10;
		if (d < 0)
			d -= d10;
		put(String.valueOf(d));
	}

}
