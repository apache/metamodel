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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.Ignore;

/**
 * Simple program used for creating large CSV datasets for performance/memory
 * testing
 */
@Ignore
class ExampleDataGenerator {

	private final int _rows;
	private final int _cols;
	private final ExampleValueGenerator _valueGenerator;
	
	public static void main(String[] args) {
		// Convenience main method. Customize if needed.
		ExampleDataGenerator gen = new ExampleDataGenerator(374635, 4, new RandomizedExampleValueGenerator(3));
		gen.createFile( new File("test1.csv"));
		gen.createFile( new File("test2.csv"));
		gen.createFile( new File("test3.csv"));
	}

	public ExampleDataGenerator(int rows, int cols) {
		this(rows, cols, new DefaultExampleValueGenerator());
	}

	public ExampleDataGenerator(int rows, int cols,
			ExampleValueGenerator valueGenerator) {
		_rows = rows;
		_cols = cols;
		_valueGenerator = valueGenerator;
	}

	public void createFile(File file) {
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(file));

			for (int j = 0; j < _cols; j++) {
				if (j != 0) {
					writer.write(',');
				}
				writer.write("col" + j);
			}

			for (int i = 0; i < _rows; i++) {
				if (i % 500000 == 0) {
					System.out.println("i: " + i);
				}
				writer.write('\n');
				for (int j = 0; j < _cols; j++) {
					if (j != 0) {
						writer.write(',');
					}
					final String value = _valueGenerator.generate(i, j);
					writer.write(value);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
