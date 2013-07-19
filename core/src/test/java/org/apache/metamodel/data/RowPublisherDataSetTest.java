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
package org.apache.metamodel.data;

import junit.framework.TestCase;

import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.util.Action;

public class RowPublisherDataSetTest extends TestCase {

	public void testMaxSize() throws Exception {
		SelectItem[] selectItems = new SelectItem[2];
		selectItems[0] = new SelectItem(new MutableColumn("foos"));
		selectItems[1] = new SelectItem(new MutableColumn("bars"));
		DataSet ds = new RowPublisherDataSet(selectItems, 5,
				new Action<RowPublisher>() {
					@Override
					public void run(RowPublisher publisher) throws Exception {

						// we want to exceed the buffer size
						int iterations = RowPublisherImpl.BUFFER_SIZE * 2;

						for (int i = 0; i < iterations; i++) {
							publisher.publish(new Object[] { "foo" + i,
									"bar" + i });
						}
					}
				});

		assertTrue(ds.next());
		assertEquals("Row[values=[foo0, bar0]]", ds.getRow().toString());
		assertTrue(ds.next());
		assertTrue(ds.next());
		assertTrue(ds.next());
		assertTrue(ds.next());
		assertEquals("Row[values=[foo4, bar4]]", ds.getRow().toString());
		assertFalse(ds.next());
		
		ds.close();
	}

	public void testExceptionInAction() throws Exception {
		SelectItem[] selectItems = new SelectItem[2];
		selectItems[0] = new SelectItem(new MutableColumn("foos"));
		selectItems[1] = new SelectItem(new MutableColumn("bars"));
		DataSet ds = new RowPublisherDataSet(selectItems, 5,
				new Action<RowPublisher>() {
					@Override
					public void run(RowPublisher publisher) throws Exception {
						publisher.publish(new Object[] { "foo0", "bar0" });
						publisher.publish(new Object[] { "foo1", "bar1" });
						throw new IllegalStateException("foobar!");
					}
				});

		assertTrue(ds.next());
		assertEquals("Row[values=[foo0, bar0]]", ds.getRow().toString());
		assertTrue(ds.next());
		assertEquals("Row[values=[foo1, bar1]]", ds.getRow().toString());

		try {
			ds.next();
			fail("Exception expected");
		} catch (Exception e) {
			assertEquals("foobar!", e.getMessage());
			assertEquals(IllegalStateException.class, e.getClass());
		} finally {
		    ds.close();
		}
	}
}
