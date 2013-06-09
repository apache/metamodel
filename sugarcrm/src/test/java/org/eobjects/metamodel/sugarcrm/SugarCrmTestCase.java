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

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import junit.framework.TestCase;

/**
 * Abstract test case which consults an external .properties file for SugarCRM
 * credentials to execute the tests.
 */
public abstract class SugarCrmTestCase extends TestCase {

    private String _username;
    private String _password;
    private int _numberOfAccounts;
    private boolean _configured;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Properties properties = new Properties();
        File file = new File(getPropertyFilePath());
        _configured = file.exists();
        if (_configured) {
            properties.load(new FileReader(file));
            _username = properties.getProperty("username");
            _password = properties.getProperty("password");
            _numberOfAccounts = Integer.parseInt(properties.getProperty("number_of_accounts"));
        }
    }

    private String getPropertyFilePath() {
        String userHome = System.getProperty("user.home");
        return userHome + "/sugarcrm-credentials.properties";
    }
    
    public int getNumberOfAccounts() {
        return _numberOfAccounts;
    }

    protected String getInvalidConfigurationMessage() {
        return "!!! WARN !!! SugarCRM ignored\r\n" + "Please configure salesforce credentials locally ("
                + getPropertyFilePath() + "), to run integration tests";
    }

    public boolean isConfigured() {
        return _configured;
    }

    public String getUsername() {
        return _username;
    }

    public String getPassword() {
        return _password;
    }
}
