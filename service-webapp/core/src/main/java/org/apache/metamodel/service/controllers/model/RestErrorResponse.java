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
package org.apache.metamodel.service.controllers.model;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the JSON object that is returned when an error occurs
 */
public class RestErrorResponse {

    @JsonProperty("code")
    private int code;

    @JsonProperty("message")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String message;

    @JsonProperty("additional_details")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> additionalDetails;

    public RestErrorResponse(int code, String message) {
        this(code, message, null);
    }

    public RestErrorResponse(int code, String message, Map<String, Object> additionalDetails) {
        this.code = code;
        this.message = message;
        this.additionalDetails = additionalDetails;
    }

    public RestErrorResponse() {
        this(-1, null, null);
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setAdditionalDetails(Map<String, Object> additionalDetails) {
        this.additionalDetails = additionalDetails;
    }

    public Map<String, Object> getAdditionalDetails() {
        return additionalDetails;
    }

}
