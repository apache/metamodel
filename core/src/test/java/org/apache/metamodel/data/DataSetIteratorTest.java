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

import org.easymock.EasyMock;

import junit.framework.TestCase;

public class DataSetIteratorTest extends TestCase {

	public void testHasNextAndNextAndClose() throws Exception {
		DataSet ds = EasyMock.createMock(DataSet.class);
		Row row = EasyMock.createMock(Row.class);

		EasyMock.expect(ds.next()).andReturn(true);
		EasyMock.expect(ds.getRow()).andReturn(row);
		EasyMock.expect(ds.next()).andReturn(true);
		EasyMock.expect(ds.getRow()).andReturn(row);
		EasyMock.expect(ds.next()).andReturn(false);
		ds.close();

		EasyMock.replay(ds, row);

		DataSetIterator it = new DataSetIterator(ds);

		// multiple hasNext calls does not iterate before next is called
		assertTrue(it.hasNext());
		assertTrue(it.hasNext());
		assertTrue(it.hasNext());

		assertSame(row, it.next());

		assertTrue(it.hasNext());
		assertTrue(it.hasNext());

		assertSame(row, it.next());
		assertFalse(it.hasNext());
		assertFalse(it.hasNext());
		assertFalse(it.hasNext());

		assertNull(it.next());

		EasyMock.verify(ds, row);
	}

	public void testRemove() throws Exception {
		DataSet ds = EasyMock.createMock(DataSet.class);
		DataSetIterator it = new DataSetIterator(ds);

		try {
			it.remove();
			fail("Exception expected");
		} catch (UnsupportedOperationException e) {
			assertEquals("DataSet is read-only, remove() is not supported.",
					e.getMessage());
		}
	}
}
