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
 * A predicate that uses an exclusion list ("black list") of elements to
 * determine whether to evaluate true or false.
 * 
 * @param <E>
 */
public class ExclusionPredicate<E> implements Predicate<E> {

    private final Collection<E> _exclusionList;

    public ExclusionPredicate(Collection<E> exclusionList) {
        _exclusionList = exclusionList;
    }

    @Override
    public Boolean eval(E arg) {
        if (_exclusionList.contains(arg)) {
            return false;
        }
        return true;
    }

    public Collection<E> getExclusionList() {
        return Collections.unmodifiableCollection(_exclusionList);
    }
}
