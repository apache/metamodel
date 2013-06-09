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
package org.eobjects.metamodel.csv;

import java.util.Random;

import org.junit.Ignore;

@Ignore
final class RandomizedExampleValueGenerator implements ExampleValueGenerator {

	private final char[] diacriticChars = new char[] { 'æ', 'ø', 'å', 'ä', 'õ',
			'â', 'á', 'í', 'ì', 'ẽ', 'ŝ', 'é', 'ç' };
	private final char[] strangeChars = new char[] { '*', '.', '~', '`', '@',
			'[', ']', '#', '-' };
	private final Random random = new Random();
	private final int letterMin = 'a';
	private final int letterMax = 'z';
	private final int letterDiff = letterMax - letterMin;
	private final int _tokenLength;

	public RandomizedExampleValueGenerator() {
		this(20);
	}

	public RandomizedExampleValueGenerator(int tokenLength) {
		_tokenLength = tokenLength;
	}

	@Override
	public String generate(int row, int col) {
		int length = random.nextInt(_tokenLength);
		if (length < 3) {
			if (random.nextInt(1000) == 0) {
				length = 0;
			} else {
				length = 1 + random.nextInt(5);
			}
		}
		char[] chars = new char[length];
		for (int i = 0; i < length; i++) {
			chars[i] = nextChar();
		}
		return String.valueOf(chars);
	}

	private char nextChar() {
		int unusualCharRandom = random.nextInt(10000);
		if (unusualCharRandom < 91) {
			return ' ';
		} else if (unusualCharRandom < 109) {
			return getRandom(diacriticChars);
		} else if (unusualCharRandom < 113) {
			return getRandom(strangeChars);
		}
		final int r = random.nextInt(letterDiff);
		char c = (char) (r + letterMin);
		if (random.nextInt(6) == 0) {
			c = Character.toUpperCase(c);
		}
		return c;
	}

	private char getRandom(char[] chars) {
		final int index = random.nextInt(chars.length);
		return chars[index];
	}

	public static void main(String[] args) {
		RandomizedExampleValueGenerator gen = new RandomizedExampleValueGenerator();
		for (int i = 0; i < 1000; i++) {
			System.out.println(gen.generate(0, 0));
		}
	}

}
