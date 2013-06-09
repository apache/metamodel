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
package org.eobjects.metamodel.util;

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
