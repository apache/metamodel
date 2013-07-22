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
package org.apache.metamodel.salesforce;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.soap.partner.fault.ExceptionCode;
import com.sforce.ws.ConnectionException;

public class SalesforceUtils {
    
    private static final Logger logger = LoggerFactory.getLogger(SalesforceUtils.class); 

    /**
     * Wraps a checked exception thrown by Salesforce into an
     * {@link IllegalStateException} with a useful message.
     * 
     * @param e
     * @param whatWentWrong
     * @return
     */
    public static IllegalStateException wrapException(ConnectionException e, String whatWentWrong) {
        logger.error("Wrapping Salesforce.com ConnectionException", e); 
        String message = null;
        Throwable cause = e;
        while (message == null && cause != null) {
            if (cause instanceof ApiFault) {
                final String exceptionMessage = ((ApiFault) cause).getExceptionMessage();
                final ExceptionCode exceptionCode = ((ApiFault) cause).getExceptionCode();
                message = exceptionCode + ": " + exceptionMessage;
                break;
            }
            cause = cause.getCause(); 
        }
        throw new IllegalStateException(whatWentWrong + ": " + message, cause);
    }
}
