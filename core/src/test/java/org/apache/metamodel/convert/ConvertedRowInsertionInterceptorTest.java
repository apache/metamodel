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
package org.apache.metamodel.convert;

import java.util.List;

import junit.framework.TestCase;

import org.apache.metamodel.MockUpdateableDataContext;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.schema.Column;

public class ConvertedRowInsertionInterceptorTest extends TestCase {

	public void testConvertedInsert() throws Exception {
		MockUpdateableDataContext source = new MockUpdateableDataContext();
		Column fooColumn = source.getColumnByQualifiedLabel("schema.table.foo");
		assertNotNull(fooColumn);

		UpdateableDataContext intercepted = Converters.addTypeConverter(source,
				fooColumn, new StringToIntegerConverter());

		final List<Object[]> values = source.getValues();

		assertEquals(3, values.size());

		intercepted.executeUpdate(new UpdateScript() {
			@Override
			public void run(UpdateCallback callback) {
				callback.insertInto("schema.table").value(0, 1).value(1, "2")
						.execute();
				callback.insertInto("schema.table").value(0, 3).value(1, "4")
						.execute();
			}
		});

		assertEquals(5, values.size());
		assertEquals("1", values.get(3)[0]);
		assertEquals("2", values.get(3)[1]);
		assertEquals("3", values.get(4)[0]);
		assertEquals("4", values.get(4)[1]);
	}
}
