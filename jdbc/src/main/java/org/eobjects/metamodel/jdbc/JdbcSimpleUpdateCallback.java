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
package org.eobjects.metamodel.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.util.FileHelper;

/**
 * Jdbc {@link UpdateCallback} for databases that do not support batch features.
 * Instead we will use a single transaction for the {@link UpdateScript}.
 * 
 * @author Kasper Sørensen
 */
final class JdbcSimpleUpdateCallback extends JdbcUpdateCallback {

    public JdbcSimpleUpdateCallback(JdbcDataContext dataContext) {
        super(dataContext);
    }
    
    @Override
    protected void closePreparedStatement(PreparedStatement preparedStatement) {
        FileHelper.safeClose(preparedStatement);
    }

    @Override
    protected void executePreparedStatement(PreparedStatement st) throws SQLException {
        st.executeUpdate();
    }
}
