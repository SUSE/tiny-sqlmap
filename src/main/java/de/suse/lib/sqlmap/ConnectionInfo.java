/*
 * Copyright 2011 SUSE Linux Products GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.suse.lib.sqlmap;

/**
 * Connection meta.
 *
 * @author Bo Maryniuk
 */
public class ConnectionInfo {
    private String tag;
    private String url;
    private String password;
    private String user;

    public ConnectionInfo(String tag, String url, String user, String password) {
        this.tag = tag;
        this.url = url;
        this.password = password;
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public String getTag() {
        return tag;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getVendor() {
        return this.getUrl().split("://")[0].replace("jdbc:", "").toLowerCase();
    }
}
