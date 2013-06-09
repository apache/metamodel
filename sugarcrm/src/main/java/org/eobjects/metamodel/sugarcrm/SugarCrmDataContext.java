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

import java.io.Closeable;
import java.net.URL;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.ws.BindingProvider;

import org.apache.commons.codec.digest.DigestUtils;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.QueryPostprocessDataContext;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.MaxRowsDataSet;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.LazyRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sugarcrm.ws.soap.EntryValue;
import com.sugarcrm.ws.soap.GetEntriesCountResult;
import com.sugarcrm.ws.soap.GetEntryListResultVersion2;
import com.sugarcrm.ws.soap.LinkNamesToFieldsArray;
import com.sugarcrm.ws.soap.NameValueList;
import com.sugarcrm.ws.soap.SelectFields;
import com.sugarcrm.ws.soap.Sugarsoap;
import com.sugarcrm.ws.soap.SugarsoapPortType;
import com.sugarcrm.ws.soap.UserAuth;

/**
 * A DataContext that uses the SugarCRM SOAP web services to fetch data from the
 * CRM system.
 */
public class SugarCrmDataContext extends QueryPostprocessDataContext implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(SugarCrmDataContext.class);

    public static final int FETCH_SIZE = 200;

    private final LazyRef<String> _sessionId;
    private final SugarsoapPortType _service;

    /**
     * 
     * @param sugarCrmBaseUrl
     *            the base URL of the SugarCRM system, e.g.
     *            http://127.0.0.1:9090/sugarcrm
     * @param username
     * @param password
     * @param applicationName
     */
    public SugarCrmDataContext(String sugarCrmBaseUrl, final String username, String password,
            final String applicationName) {
        if (sugarCrmBaseUrl.endsWith("/")) {
            // remove trailing slashes
            sugarCrmBaseUrl = sugarCrmBaseUrl.substring(0, sugarCrmBaseUrl.length() - 1);
        }

        final String endpointAddress = sugarCrmBaseUrl + "/service/v4/soap.php";
        final String wsdlAddress = endpointAddress + "?wsdl";

        logger.info("Connecting to SugarCRM SOAP service using WSDL URL: {}", wsdlAddress);

        final URL wsdlUrl;
        try {
            wsdlUrl = new URL(wsdlAddress);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid SugarCRM base URL: " + e.getMessage(), e);
        }

        final Sugarsoap soap = new Sugarsoap(wsdlUrl, new QName("http://www.sugarcrm.com/sugarcrm", "sugarsoap"));
        _service = soap.getSugarsoapPort();

        assert _service instanceof BindingProvider;
        final BindingProvider bindingProvider = (BindingProvider) _service;
        bindingProvider.getRequestContext().put(BindingProvider.ENDPOINT_ADDRESS_PROPERTY, endpointAddress);

        final String md5password = DigestUtils.md5Hex(password);
        _sessionId = new LazyRef<String>() {
            @Override
            protected String fetch() {
                UserAuth userAuth = new UserAuth();
                userAuth.setUserName(username);
                userAuth.setPassword(md5password);
                logger.debug("Logging in as '{}', with application name '{}'", username, applicationName);
                EntryValue response = _service.login(userAuth, applicationName, new NameValueList());
                String sessionId = response.getId();
                logger.info("Started session with SugarCRM. Session ID: {}", sessionId);
                return sessionId;
            }
        };
    }

    @Override
    public void close() {
        if (_sessionId.isFetched()) {
            try {
                _service.logout(_sessionId.get());
            } catch (Exception e) {
                logger.debug("Failed to log out while closing DataContext", e);
            }
        }
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        Schema schema = new SugarCrmSchema(getMainSchemaName(), _service, _sessionId);
        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return "SugarCRM";
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        if (whereItems.isEmpty()) {
            final String session = _sessionId.get();
            final String moduleName = table.getName();
            final GetEntriesCountResult entriesCount = _service.getEntriesCount(session, moduleName, "", 0);
            final int resultCount = entriesCount.getResultCount();
            return resultCount;
        }
        return super.executeCountQuery(table, whereItems, functionApproximationAllowed);
    }

    @Override
    protected DataSet materializeMainSchemaTable(final Table table, final Column[] columns, final int maxRows) {

        final String session = _sessionId.get();
        final String moduleName = table.getName();

        final SelectFields selectFields = SugarCrmXmlHelper.createSelectFields(columns);

        final LinkNamesToFieldsArray linkNameToFieldsArray = new LinkNamesToFieldsArray();

        final int fetchSize;
        if (maxRows < 0 || maxRows > FETCH_SIZE) {
            fetchSize = FETCH_SIZE;
        } else {
            fetchSize = maxRows;
        }

        final GetEntryListResultVersion2 entryList = _service.getEntryList(session, moduleName, "", "", 0,
                selectFields, linkNameToFieldsArray, fetchSize, 0, false);

        final SugarCrmDataSet dataSet = new SugarCrmDataSet(columns, _service, session, entryList);
        
        if (maxRows > 0) {
            // sugar's responses are a bit weird to interpret regarding total count, so we apply a MaxRowsDataSet wrapper.
            return new MaxRowsDataSet(dataSet, maxRows);
        }
        
        return dataSet;
    }
}