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
package org.apache.metamodel.jdbc.dialects;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is responsible for parsing version numbers in software products. Version strings are expected to be
 * numeric and dot-separated.
 */
class VersionParser {
    private static final Pattern versionPattern = Pattern.compile("[0-9]+(\\.[0-9]+)+");

    private VersionParser() {
    }

    /**
     * @param stringToParse the string that contains the version.
     * @return If the version exists returns the full version else empty string.
     */
    public static String getVersion(String stringToParse) {
        if (stringToParse != null) {
            Matcher matcher = versionPattern.matcher(stringToParse);
            if (matcher.find()) {
                return matcher.group();
            }
        }
        return "";
    }

    /**
     * @param stringToParse the string that contains the version.
     * @return If the version exists returns only the number(s) before the first dot else 0.
     */
    public static int getMajorVersion(String stringToParse) {
        String fullVersion = getVersion(stringToParse);
        if (fullVersion != null) {
            int firstDot = fullVersion.indexOf('.');
            if (firstDot >= 0) {
                return Integer.valueOf(fullVersion.substring(0, firstDot));
            }
        }
        return -1;
    }
}
