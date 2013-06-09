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

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.delete.AbstractRowDeletionBuilder;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.Action;
import org.eobjects.metamodel.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class CsvDeleteBuilder extends AbstractRowDeletionBuilder {

    private static final Logger logger = LoggerFactory.getLogger(CsvDeleteBuilder.class);

    private final CsvUpdateCallback _updateCallback;

    public CsvDeleteBuilder(CsvUpdateCallback updateCallback, Table table) {
        super(table);
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() throws MetaModelException {
        final File tempFile = FileHelper.createTempFile("metamodel_deletion", "csv");

        final CsvConfiguration configuration = _updateCallback.getConfiguration();

        final CsvDataContext copyDataContext = new CsvDataContext(tempFile, configuration);
        copyDataContext.executeUpdate(new UpdateScript() {

            @Override
            public void run(UpdateCallback callback) {
                final Table originalTable = getTable();
                final Table copyTable = callback.createTable(copyDataContext.getDefaultSchema(), originalTable.getName())
                        .like(originalTable).execute();

                if (isTruncateTableOperation()) {
                    // no need to iterate old records, they should all be
                    // removed
                    return;
                }

                final DataSet dataSet = _updateCallback.getDataContext().query().from(originalTable)
                        .select(originalTable.getColumns()).execute();
                try {
                    while (dataSet.next()) {
                        final Row row = dataSet.getRow();
                        if (!deleteRow(row)) {
                            callback.insertInto(copyTable).like(row).execute();
                        }
                    }
                } finally {
                    dataSet.close();
                }
            }
        });

        // copy the copy (which does not have deleted records) to overwrite the
        // original
        final InputStream in = FileHelper.getInputStream(tempFile);
        try {
            _updateCallback.getResource().write(new Action<OutputStream>() {
                @Override
                public void run(OutputStream out) throws Exception {
                    FileHelper.copy(in, out);
                }
            });
        } finally {
            FileHelper.safeClose(in);
        }

        final boolean deleted = tempFile.delete();
        if (!deleted) {
            logger.warn("Could not delete temporary copy-file: {}", tempFile);
        }
    }
}
