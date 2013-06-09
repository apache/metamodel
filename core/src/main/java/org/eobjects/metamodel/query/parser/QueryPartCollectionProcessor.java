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
package org.eobjects.metamodel.query.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple implementation of {@link QueryPartProcessor} which simply adds all
 * elements to a collection. Use {@link #getTokens()} to retrieve the 'processed'
 * tokens and {@link #getDelims()} for the corresponding delimitors.
 */
public class QueryPartCollectionProcessor implements QueryPartProcessor {

    private final List<String> _delims;
    private final List<String> _tokens;

    public QueryPartCollectionProcessor() {
        _tokens = new ArrayList<String>();
        _delims = new ArrayList<String>();
    }

    @Override
    public void parse(String delim, String token) {
        _delims.add(delim);
        _tokens.add(token);
    }
    
    public List<String> getDelims() {
        return _delims;
    }

    public List<String> getTokens() {
        return _tokens;
    }
}
