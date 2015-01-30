/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.schema;

/**
 * Abstract {@link Column} implementation. Implements most common and trivial
 * methods.
 */
public abstract class AbstractColumn implements Column {

    private static final long serialVersionUID = 1L;

    @Override
    public final String getQuotedName() {
        String quote = getQuote();
        if (quote == null) {
            return getName();
        }
        return quote + getName() + quote;
    }

    @Override
    public final String getQualifiedLabel() {
        StringBuilder sb = new StringBuilder();
        Table table = getTable();
        if (table != null) {
            sb.append(table.getQualifiedLabel());
            sb.append('.');
        }
        sb.append(getName());
        return sb.toString();
    }

    @Override
    public final int compareTo(Column that) {
        int diff = getQualifiedLabel().compareTo(that.getQualifiedLabel());
        if (diff == 0) {
            diff = toString().compareTo(that.toString());
        }
        return diff;
    }

    @Override
    public final String toString() {
        return "Column[name=" + getName() + ",columnNumber=" + getColumnNumber() + ",type=" + getType() + ",nullable="
                + isNullable() + ",nativeType=" + getNativeType() + ",columnSize=" + getColumnSize() + "]";
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj instanceof Column) {
            Column other = (Column) obj;
            if (getColumnNumber() != other.getColumnNumber()) {
                return false;
            }
            
            if (!getName().equals(other.getName())) {
                return false;
            }
            if (getType() != other.getType()) {
                return false;
            }

            final Table table1 = getTable();
            final Table table2 = other.getTable();
            if (table1 == null) {
                if (table2 != null) {
                    return false;
                }
            } else {
                if (!table1.equals(table2)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}