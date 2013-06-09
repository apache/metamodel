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

import org.eobjects.metamodel.MetaModelException;

/**
 * Exception type for errors that occur while dealing with {@link Resource}s.
 */
public class ResourceException extends MetaModelException {

    private static final long serialVersionUID = 1L;
    
    private final Resource _resource;

    public ResourceException(Resource resource, Exception cause) {
        super(cause);
        _resource = resource;
    }

    public ResourceException(Resource resource,String message, Exception cause) {
        super(message, cause);
        _resource = resource;
    }

    public ResourceException(Resource resource,String message) {
        super(message);
        _resource = resource;
    }
    
    public Resource getResource() {
        return _resource;
    }
}
