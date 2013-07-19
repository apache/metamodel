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
package org.apache.metamodel.pojo;

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
