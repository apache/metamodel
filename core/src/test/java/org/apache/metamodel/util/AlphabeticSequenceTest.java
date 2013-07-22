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
package org.apache.metamodel.util;

import junit.framework.TestCase;

public class AlphabeticSequenceTest extends TestCase {
	
	public void testNoArgsConstructor() throws Exception {
		AlphabeticSequence seq = new AlphabeticSequence();
		assertEquals("A", seq.next());
	}

	public void testNext() throws Exception {
		AlphabeticSequence seq = new AlphabeticSequence("A");
		assertEquals("A", seq.current());
		assertEquals("B", seq.next());
		assertEquals("C", seq.next());
		assertEquals("D", seq.next());
		assertEquals("E", seq.next());
		assertEquals("F", seq.next());
		assertEquals("G", seq.next());
		assertEquals("H", seq.next());
		assertEquals("I", seq.next());
		assertEquals("J", seq.next());
		assertEquals("K", seq.next());
		assertEquals("L", seq.next());
		assertEquals("M", seq.next());
		assertEquals("N", seq.next());
		assertEquals("O", seq.next());
		assertEquals("P", seq.next());
		assertEquals("Q", seq.next());
		assertEquals("R", seq.next());
		assertEquals("S", seq.next());
		assertEquals("T", seq.next());
		assertEquals("U", seq.next());
		assertEquals("V", seq.next());
		assertEquals("W", seq.next());
		assertEquals("X", seq.next());
		assertEquals("Y", seq.next());
		assertEquals("Z", seq.next());
		assertEquals("AA", seq.next());
		
		seq = new AlphabeticSequence("AZ");
		assertEquals("BA", seq.next());
		
		seq = new AlphabeticSequence("ZZ");
		assertEquals("AAA", seq.next());
		
		seq = new AlphabeticSequence("ABZ");
		assertEquals("ACA", seq.next());
	}
}
