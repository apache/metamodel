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
package org.apache.metamodel.service.controllers;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.metamodel.query.parser.QueryParserException;
import org.apache.metamodel.service.app.exceptions.AbstractIdentifierNamingException;
import org.apache.metamodel.service.app.exceptions.DataSourceAlreadyExistException;
import org.apache.metamodel.service.app.exceptions.DataSourceNotUpdateableException;
import org.apache.metamodel.service.app.exceptions.NoSuchColumnException;
import org.apache.metamodel.service.app.exceptions.NoSuchDataSourceException;
import org.apache.metamodel.service.app.exceptions.NoSuchSchemaException;
import org.apache.metamodel.service.app.exceptions.NoSuchTableException;
import org.apache.metamodel.service.app.exceptions.NoSuchTenantException;
import org.apache.metamodel.service.app.exceptions.TenantAlreadyExistException;
import org.apache.metamodel.service.controllers.model.RestErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

@ControllerAdvice
public class RestErrorHandler {

    /**
     * Method binding issues (raised by Spring framework) - mapped to
     * BAD_REQUEST.
     * 
     * @param ex
     * @return
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ResponseBody
    public RestErrorResponse processValidationError(MethodArgumentNotValidException ex) {
        final BindingResult result = ex.getBindingResult();

        final Map<String, Object> globalErrorsMap = new LinkedHashMap<>();
        final List<ObjectError> globalErrors = result.getGlobalErrors();
        for (ObjectError objectError : globalErrors) {
            globalErrorsMap.put(objectError.getObjectName(), objectError.getDefaultMessage());
        }

        final List<FieldError> fieldErrors = result.getFieldErrors();
        final Map<String, Object> fieldErrorsMap = new LinkedHashMap<>();
        for (FieldError fieldError : fieldErrors) {
            fieldErrorsMap.put(fieldError.getObjectName() + '.' + fieldError.getField(), fieldError
                    .getDefaultMessage());
        }

        final Map<String, Object> additionalDetails = new LinkedHashMap<>();
        if (!globalErrorsMap.isEmpty()) {
            additionalDetails.put("global-errors", globalErrorsMap);
        }
        if (!fieldErrorsMap.isEmpty()) {
            additionalDetails.put("field-errors", fieldErrorsMap);
        }
        final RestErrorResponse errorResponse = new RestErrorResponse(HttpStatus.BAD_REQUEST.value(),
                "Failed to validate request");
        if (!additionalDetails.isEmpty()) {
            errorResponse.setAdditionalDetails(additionalDetails);
        }
        return errorResponse;
    }

    /**
     * No such [Entity] exception handler method - mapped to NOT_FOUND.
     * 
     * @param ex
     * @return
     */
    @ExceptionHandler({ NoSuchTenantException.class, NoSuchDataSourceException.class, NoSuchSchemaException.class,
            NoSuchTableException.class, NoSuchColumnException.class })
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ResponseBody
    public RestErrorResponse processNoSuchEntity(AbstractIdentifierNamingException ex) {
        return new RestErrorResponse(HttpStatus.NOT_FOUND.value(), "Not found: " + ex.getIdentifier());
    }

    /**
     * [Entity] already exist exception handler method - mapped to CONFLICT.
     * 
     * @param ex
     * @return
     */
    @ExceptionHandler({ TenantAlreadyExistException.class, DataSourceAlreadyExistException.class })
    @ResponseStatus(HttpStatus.CONFLICT)
    @ResponseBody
    public RestErrorResponse processEntityAlreadyExist(AbstractIdentifierNamingException ex) {
        return new RestErrorResponse(HttpStatus.CONFLICT.value(), "Already exist: " + ex.getIdentifier());
    }

    /**
     * DataSource not updateable exception handler method - mapped to
     * BAD_REQUEST.
     * 
     * @param ex
     * @return
     */
    @ExceptionHandler(DataSourceNotUpdateableException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ResponseBody
    public RestErrorResponse processDataSourceNotUpdateable(DataSourceNotUpdateableException ex) {
        return new RestErrorResponse(HttpStatus.BAD_REQUEST.value(), "DataSource not updateable: " + ex
                .getDataSourceName());
    }

    /**
     * Query parsing exception - mapped to BAD_REQUEST.
     * 
     * @param ex
     * @return
     */
    @ExceptionHandler(QueryParserException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ResponseBody
    public RestErrorResponse processQueryParsingError(QueryParserException ex) {
        return new RestErrorResponse(HttpStatus.BAD_REQUEST.value(), ex.getMessage());
    }

    /**
     * Catch-all exception handler method - mapped to INTERNAL_SERVER_ERROR.
     * 
     * @param ex
     * @return
     */
    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    public RestErrorResponse processAnyException(Exception ex) {
        final Map<String, Object> additionalDetails = new HashMap<>();
        additionalDetails.put("exception_type", ex.getClass().getName());
        return new RestErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR.value(), ex.getMessage(), additionalDetails);
    }
}
