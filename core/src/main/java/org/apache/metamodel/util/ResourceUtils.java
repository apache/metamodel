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
package org.apache.metamodel.util;

import java.net.URI;

import org.apache.metamodel.factory.ResourceFactoryRegistryImpl;
import org.apache.metamodel.factory.ResourceProperties;
import org.apache.metamodel.factory.SimpleResourceProperties;
import org.apache.metamodel.factory.UnsupportedResourcePropertiesException;

/**
 * Static utility methods for handling {@link Resource}s.
 */
public class ResourceUtils {

    /**
     * Creates a Resource based on a URI
     * 
     * @param uri
     * @return
     * @throws UnsupportedResourcePropertiesException
     *             if the scheme or other part of the URI is unsupported.
     */
    public static Resource toResource(URI uri) throws UnsupportedResourcePropertiesException {
        return toResource(new SimpleResourceProperties(uri));
    }

    /**
     * Creates a Resource based on a path or URI (represented by a String)
     * 
     * @param uri
     * @return
     * @throws UnsupportedResourcePropertiesException
     *             if the scheme or other part of the string is unsupported.
     */
    public static Resource toResource(String uri) throws UnsupportedResourcePropertiesException {
        return toResource(new SimpleResourceProperties(uri));
    }

    /**
     * Creates a Resource based on the {@link ResourceProperties} definition.
     * 
     * @param resourceProperties
     * @return
     * @throws UnsupportedResourcePropertiesException
     *             if the provided properties cannot be handled in creation of a
     *             resource.
     */
    public static Resource toResource(ResourceProperties resourceProperties)
            throws UnsupportedResourcePropertiesException {
        return ResourceFactoryRegistryImpl.getDefaultInstance().createResource(resourceProperties);
    }

    /**
     * Gets the parent name of a resource. For example, if the resource's
     * qualified path is /foo/bar/baz, this method will return "bar".
     * 
     * @param resource
     * @return
     */
    public static String getParentName(Resource resource) {
        String name = resource.getName();
        String qualifiedPath = resource.getQualifiedPath();

        assert qualifiedPath.endsWith(name);

        int indexOfChild = qualifiedPath.length() - name.length();

        if (indexOfChild <= 0) {
            return "";
        }

        String parentQualifiedPath = qualifiedPath.substring(0, indexOfChild);

        if ("/".equals(parentQualifiedPath)) {
            return parentQualifiedPath;
        }

        parentQualifiedPath = parentQualifiedPath.substring(0, parentQualifiedPath.length() - 1);

        int lastIndexOfSlash = parentQualifiedPath.lastIndexOf('/');
        int lastIndexOfBackSlash = parentQualifiedPath.lastIndexOf('\\');
        int lastIndexToUse = Math.max(lastIndexOfSlash, lastIndexOfBackSlash);

        if (lastIndexToUse == -1) {
            return parentQualifiedPath;
        }

        // add one because of the slash/backslash itself
        // lastIndexToUse++;

        String parentName = parentQualifiedPath.substring(lastIndexToUse + 1);
        return parentName;
    }
}
