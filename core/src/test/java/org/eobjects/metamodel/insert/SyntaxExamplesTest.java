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
package org.eobjects.metamodel.insert;

import junit.framework.TestCase;

import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateableDataContext;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;

public abstract class SyntaxExamplesTest extends TestCase {

	private UpdateableDataContext dc;
	private Table table;
	private Column col;

	public void testInsertMultipleRows() throws Exception {
		dc.executeUpdate(new UpdateScript() {
			@Override
			public void run(UpdateCallback cb) {
				cb.insertInto(table).value(col, "foo").execute();
				cb.insertInto(table).value(col, "bar").execute();
			}
		});
	}
}