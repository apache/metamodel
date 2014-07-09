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
package org.apache.metamodel.schema.builder;

import java.util.Map;

import org.apache.metamodel.util.LazyRef;

/**
 * A {@link DocumentSource} that is lazy loaded. Using this as a wrapper around
 * another source may save resources, since a {@link DocumentSource} is not
 * always invoked and thus the initial creation and closing of the source can
 * sometimes be avoided.
 */
public class LazyDocumentSource implements DocumentSource {

    private final LazyRef<DocumentSource> _lazyRef;

    public LazyDocumentSource(LazyRef<DocumentSource> lazyRef) {
        _lazyRef = lazyRef;
    }

    @Override
    public Map<String, ?> next() {
        return _lazyRef.get().next();
    }

    @Override
    public void close() {
        if (_lazyRef.isFetched()) {
            _lazyRef.get().close();
        }
    }

}
