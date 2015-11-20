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
package org.apache.metamodel.elasticsearch.rest;

import io.searchbox.action.GenericResultAbstractAction;

public class JestDeleteScroll extends GenericResultAbstractAction {
    private JestDeleteScroll(Builder builder) {
        super(builder);
        this.payload = builder.getScrollId();
        setURI(buildURI());
    }

    @Override
    public String getRestMethodName() {
        return "DELETE";
    }

    @Override
    protected String buildURI() {
        return super.buildURI() + "/_search/scroll";
    }

    public static class Builder extends GenericResultAbstractAction.Builder<JestDeleteScroll, Builder> {
        private final String scrollId;

        public Builder(String scrollId) {
            this.scrollId = scrollId;
        }

        @Override
        public JestDeleteScroll build() {
            return new JestDeleteScroll(this);
        }

        public String getScrollId() {
            return scrollId;
        }
    }

}
