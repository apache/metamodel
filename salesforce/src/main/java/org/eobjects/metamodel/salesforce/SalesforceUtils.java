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
package org.eobjects.metamodel.salesforce;

import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.soap.partner.fault.ExceptionCode;
import com.sforce.ws.ConnectionException;

public class SalesforceUtils {

    /**
     * Wraps a checked exception thrown by Salesforce into an
     * {@link IllegalStateException} with a useful message.
     * 
     * @param e
     * @param whatWentWrong
     * @return
     */
    public static IllegalStateException wrapException(ConnectionException e, String whatWentWrong) {
        String message = null;
        Throwable cause = e;
        while (message == null && cause != null) {
            if (cause instanceof ApiFault) {
                final String exceptionMessage = ((ApiFault) cause).getExceptionMessage();
                final ExceptionCode exceptionCode = ((ApiFault) cause).getExceptionCode();
                message = exceptionCode + ": " + exceptionMessage;
                break;
            }
        }
        throw new IllegalStateException(whatWentWrong + ": " + message, cause);
    }
}
