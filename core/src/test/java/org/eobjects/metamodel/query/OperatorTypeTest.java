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
package org.eobjects.metamodel.query;

import junit.framework.TestCase;

public class OperatorTypeTest extends TestCase {

    public void testConvertOperatorType() throws Exception {
        assertEquals(OperatorType.EQUALS_TO, OperatorType.convertOperatorType("="));
        assertEquals(OperatorType.GREATER_THAN, OperatorType.convertOperatorType(">"));
        assertEquals(OperatorType.LESS_THAN, OperatorType.convertOperatorType("<"));
        assertEquals(OperatorType.DIFFERENT_FROM, OperatorType.convertOperatorType("<>"));
        assertEquals(OperatorType.LIKE, OperatorType.convertOperatorType("LIKE"));
        assertEquals(OperatorType.IN, OperatorType.convertOperatorType("IN"));
        assertEquals(null, OperatorType.convertOperatorType("foo"));
    }
}
