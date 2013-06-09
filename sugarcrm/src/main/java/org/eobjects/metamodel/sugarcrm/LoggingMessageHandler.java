/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel.sugarcrm;

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
