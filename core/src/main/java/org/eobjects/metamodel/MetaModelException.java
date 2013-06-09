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

/**
 * Unchecked exception used to signal errors occuring in MetaModel.
 * 
 * All MetaModelExceptions represent errors discovered withing the MetaModel
 * framework. Typically these will occur if you have put together a query that
 * is not meaningful or if there is a structural problem in a schema.
 */
public class MetaModelException extends RuntimeException {

	private static final long serialVersionUID = 5455738384633428319L;

	public MetaModelException(String message, Exception cause) {
		super(message, cause);
	}

	public MetaModelException(String message) {
		super(message);
	}

	public MetaModelException(Exception cause) {
		super(cause);
	}

	public MetaModelException() {
		super();
	}
}