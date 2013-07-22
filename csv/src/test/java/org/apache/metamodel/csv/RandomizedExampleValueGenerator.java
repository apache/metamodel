/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.csv;

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
