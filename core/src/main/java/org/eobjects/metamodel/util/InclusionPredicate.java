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

import java.util.Collection;
import java.util.Collections;

/**
 * A predicate that uses an inclusion list ("white list") of elements to
 * determine whether to evaluate true or false.
 * 
 * @param <E>
 */
public class InclusionPredicate<E> implements Predicate<E> {

    private final Collection<E> _inclusionList;

    public InclusionPredicate(Collection<E> inclusionList) {
        _inclusionList = inclusionList;
    }

    @Override
    public Boolean eval(E arg) {
        if (_inclusionList.contains(arg)) {
            return true;
        }
        return false;
    }
    
    public Collection<E> getInclusionList() {
        return Collections.unmodifiableCollection(_inclusionList);
    }

}
