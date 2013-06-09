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

import java.io.IOException;
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
public class DBT_iii extends DBTFile {

	public DBT_iii(DBF iDBF, boolean readOnly) throws IOException,
			xBaseJException {
		super(iDBF, readOnly, DBF.DBASEIII_WITH_MEMO);
	}

	public DBT_iii(DBF iDBF, String name, boolean destroy) throws IOException,
			xBaseJException {
		super(iDBF, name, destroy, DBF.DBASEIII_WITH_MEMO);
	}

	public void setNextBlock() throws IOException {

		if (file.length() == 0) {

			file.writeInt(DbaseUtils.x86(1));
			nextBlock = 1;
			file.seek(511);
			file.writeByte(0);
		} else {
			nextBlock = DbaseUtils.x86(file.readInt());
		}

	}

	public byte[] readBytes(byte[] input) throws IOException, xBaseJException {

		byte[] bTemp = new byte[513];
		boolean work = true;
		boolean onefound = false;
		byte[] bTemp2 = null;
		byte[] bTemp3 = null;
		int workLength = 0;

		for (int i = 0; i < 10; i++) {
			if (input[i] >= BYTEZERO && input[i] <= '9')
				break;
			input[i] = BYTEZERO;
		}

		String sPos;
		sPos = new String(input, 0, 10);
		long lPos = Long.parseLong(sPos);
		if (lPos == 0)
			return null;
		file.seek(lPos * memoBlockSize);
		int i;

		do {
			file.read(bTemp, 0, memoBlockSize);
			for (i = 0; i < memoBlockSize; i++) {
				if (bTemp[i] == 0x1a) {
					if (onefound == true) {
						work = false;
						bTemp[i] = 0;
						i--;
						break;
					}
					work = false;
					onefound = true;
					break;
				} else if (bTemp[i] == 0x00) {
					if (onefound == true) {
						work = false;
						break;
					}
					onefound = false;
				} else
					onefound = false;
			}
			if (workLength > 0) {
				bTemp3 = new byte[workLength];
				System.arraycopy(bTemp2, 0, bTemp3, 0, workLength);
			}
			bTemp2 = new byte[workLength + i];
			if (workLength > 0)
				System.arraycopy(bTemp3, 0, bTemp2, 0, workLength);
			System.arraycopy(bTemp, 0, bTemp2, workLength, i);
			workLength += i;

			if (workLength > file.length())
				throw new xBaseJException(
						"error reading dtb file, reading exceeds length of file");
		} while (work);
		return bTemp2;
	}

	public byte[] write(String value, int originalSize, boolean write,
			byte originalPos[]) throws IOException, xBaseJException {
		boolean madebigger;
		long startPos;
		int pos;
		byte buffer[] = new byte[512];

		if (value.length() == 0) {
			byte breturn[] = { BYTEZERO, BYTEZERO, BYTEZERO, BYTEZERO,
					BYTEZERO, BYTEZERO, BYTEZERO, BYTEZERO, BYTEZERO, BYTEZERO };
			return breturn;
		}

		if ((originalSize == 0) && (value.length() > 0))
			madebigger = true;
		else if (((value.length() / memoBlockSize) + 1) > ((originalSize / memoBlockSize) + 1))
			madebigger = true;
		else
			madebigger = false;

		if (madebigger || write) {
			startPos = nextBlock;
			nextBlock += ((value.length() + 2) / memoBlockSize) + 1;
		} else {
			String sPos;
			sPos = new String(originalPos, 0, 10);
			startPos = Long.parseLong(sPos);
		} /* endif */

		file.seek(startPos * memoBlockSize);

		for (pos = 0; pos < value.length(); pos += memoBlockSize) {
			byte b[];
			if ((pos + memoBlockSize) > value.length()) {
				try {
					b = value.substring(pos, value.length()).getBytes(
							DBF.encodedType);
				} catch (UnsupportedEncodingException UEE) {
					b = value.substring(pos, value.length()).getBytes();
				}
			} else {
				try {
					b = value.substring(pos, (pos + memoBlockSize)).getBytes(
							DBF.encodedType);
				} catch (UnsupportedEncodingException UEE) {
					b = value.substring(pos, (pos + memoBlockSize)).getBytes();
				}
			}

			for (int x = 0; x < b.length; x++)
				buffer[x] = b[x];

			file.write(buffer, 0, 512);
		} /* endfor */

		file.seek((startPos * memoBlockSize) + value.length());
		file.writeByte(26);
		file.writeByte(26);

		if (madebigger || write) {
			file.seek((memoBlockSize * nextBlock) - 1);
			file.writeByte(26);
			file.seek(0);
			file.writeInt(DbaseUtils.x86(nextBlock));
		}

		String returnString = new String(Long.toString(startPos));

		byte ten[] = new byte[10];
		byte newTen[] = new byte[10];
		newTen = returnString.getBytes();

		for (pos = 0; pos < (10 - returnString.length()); pos++)
			ten[pos] = BYTEZERO;

		for (int x = 0; pos < 10; pos++, x++)
			ten[pos] = newTen[x];

		return ten;
	}

}
