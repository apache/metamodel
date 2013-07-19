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
