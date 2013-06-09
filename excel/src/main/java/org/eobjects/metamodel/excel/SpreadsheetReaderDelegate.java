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
package org.eobjects.metamodel.excel;

import java.io.InputStream;

import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.Ref;

/**
 * Delegate for spreadsheet-implementation specific operations in an
 * {@link ExcelDataContext}.
 * 
 * @author Kasper Sørensen
 */
interface SpreadsheetReaderDelegate {

	public void notifyTablesModified(Ref<InputStream> inputStreamRef);

	public Schema createSchema(InputStream inputStream, String schemaName)
			throws Exception;

	public DataSet executeQuery(InputStream inputStream, Table table,
			Column[] columns, int maxRows) throws Exception;

}
