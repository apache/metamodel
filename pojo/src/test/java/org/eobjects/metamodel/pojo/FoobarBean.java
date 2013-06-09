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
package org.eobjects.metamodel.pojo;

import java.io.Serializable;

/**
 * An example POJO used by the {@link ObjectTableDataProviderTest} and
 * {@link PojoDataContextTest}.
 */
public class FoobarBean implements Serializable {

    private static final long serialVersionUID = 1L;

    private String col1;
    private Integer col2;
    private Boolean col3;

    public FoobarBean() {
    }

    public FoobarBean(String col1, int col2, boolean col3) {
        this.col1 = col1;
        this.col2 = col2;
        this.col3 = col3;
    }

    public String getCol1() {
        return col1;
    }

    public void setCol1(String col1) {
        this.col1 = col1;
    }

    public Integer getCol2() {
        return col2;
    }

    public void setCol2(Integer col2) {
        this.col2 = col2;
    }

    public Boolean getCol3() {
        return col3;
    }

    public void setCol3(Boolean col3) {
        this.col3 = col3;
    }
}
