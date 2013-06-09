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
