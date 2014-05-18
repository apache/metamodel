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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * A specialized {@link ObjectInputStream} for MetaModel which can be used or
 * extended if it is needed to deserialize legacy MetaModel objects. This is
 * needed since the namespace of MetaModel was changed from
 * org.eobjects.metamodel to org.apache.metamodel.
 */
public class LegacyDeserializationObjectInputStream extends ObjectInputStream {

    public LegacyDeserializationObjectInputStream(InputStream in) throws IOException, SecurityException {
        super(in);
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        String className = desc.getName();
        if (className.startsWith("org.eobjects.metamodel") || className.startsWith("[Lorg.eobjects.metamodel")) {
            String newClassName = className.replace("org.eobjects", "org.apache");
            return Class.forName(newClassName);
        }
        return super.resolveClass(desc);
    }
}
