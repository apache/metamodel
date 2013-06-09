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
package org.eobjects.metamodel;

import java.util.Comparator;

/**
 * Comparator for comparing schema names.
 * 
 * @author Kasper Sørensen
 */
class SchemaNameComparator implements Comparator<String> {

    private static Comparator<? super String> _instance = new SchemaNameComparator();

    public static Comparator<? super String> getInstance() {
        return _instance;
    }

    private SchemaNameComparator() {
    }

    public int compare(String o1, String o2) {
        if (o1 == null && o2 == null) {
            return 0;
        }
        if (o1 == null) {
            return -1;
        }
        if (o2 == null) {
            return 1;
        }
        if (MetaModelHelper.isInformationSchema(o1)) {
            return -1;
        }
        if (MetaModelHelper.isInformationSchema(o2)) {
            return 1;
        }
        return o1.compareTo(o2);
    }

}