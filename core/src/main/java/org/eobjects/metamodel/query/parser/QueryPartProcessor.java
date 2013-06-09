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

/**
 * Callback of the {@link QueryPartParser}, which recieves notifications
 * whenever a token is identified/parsed. A {@link QueryPartProcessor} is used
 * to perform the actual processing of identified tokens.
 */
public interface QueryPartProcessor {

    /**
     * Method invoked whenever the {@link QueryPartParser} identifies a token.
     * 
     * @param delim
     *            the (previous) delimitor identified before the token. This
     *            will always be null in case of the first token.
     * @param token
     *            the token identified.
     */
    public void parse(String delim, String token);
}
