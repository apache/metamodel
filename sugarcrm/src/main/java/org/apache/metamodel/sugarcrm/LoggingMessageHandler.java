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
package org.apache.metamodel.sugarcrm;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.Set;

import javax.xml.namespace.QName;
import javax.xml.soap.SOAPMessage;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.handler.soap.SOAPHandler;
import javax.xml.ws.handler.soap.SOAPMessageContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SOAP message handler that will do debug logging of request and response
 * payloads.
 */
public class LoggingMessageHandler implements SOAPHandler<SOAPMessageContext> {

    private static final Logger logger = LoggerFactory.getLogger(LoggingMessageHandler.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<QName> getHeaders() {
        return Collections.emptySet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close(MessageContext context) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean handleFault(SOAPMessageContext context) {
        if (logger.isDebugEnabled()) {
            String messageAsString = getMessageAsString(context.getMessage());
            logger.debug("Received fault message: {}", messageAsString);
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean handleMessage(SOAPMessageContext context) {
        if (logger.isDebugEnabled()) {
            String messageAsString = getMessageAsString(context.getMessage());
            logger.debug("Handling message: {}", messageAsString);
        }
        return true;
    }
    
    /**
     * Gets a complete SOAP message as a string (use with caution, since
     * messages can be large!)
     * 
     * @param context
     * @return
     */
    private String getMessageAsString(SOAPMessage message) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            message.writeTo(baos);
        } catch (Exception e) {
            logger.error("Unexpected error while getting message", e);
        }
        String messageAsString = new String(baos.toByteArray());
        return messageAsString;
    }
}
