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

package de.suse.lib.sqlmap.drivers;

import de.suse.lib.sqlmap.ConnectionInfo;
import java.io.File;
import java.net.URISyntaxException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Apache Derby in embedded mode.
 * 
 * @author Bo Maryniuk
 */
public class ApacheDerbyEmbeddedDriver extends GenericDriver {
    private final static String PROTO = "jdbc:derby://";
    private final static String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    private ConnectionInfo info;


    public ApacheDerbyEmbeddedDriver(ConnectionInfo info)
            throws URISyntaxException,
                   ClassNotFoundException,
                   SQLException,
                   Exception {
        super(info.getUrl());
        this.info = info;
        Class.forName(ApacheDerbyEmbeddedDriver.DRIVER);
    }


    @Override
    protected void parseURL() throws SQLException {
        if (this.getUrl().startsWith(ApacheDerbyEmbeddedDriver.PROTO)) {
            String host = this.getUrl().replace(ApacheDerbyEmbeddedDriver.PROTO, "").split("/")[0]; // "-" is for embedded.
            if (host.equals("-")) {
                this.setDatabase(this.getUrl().replace(ApacheDerbyEmbeddedDriver.PROTO, "").split("-", 0)[1]);
            } else {
                throw new SQLException("Apache Derby supported only in embedded mode.");
            }
        } else {
            throw new SQLException("URL has no " + this.getClass().getName() + " protocol signature.");
        }
    }


    @Override
    public ApacheDerbyEmbeddedDriver connect(String user, String password) throws Exception {
        if (this.getDatabase() == null || this.getDatabase().isEmpty()) {
            throw new Exception("Unknown database.");
        }

        try {
            String[] databaseUrlTokens = this.getDatabase().split(";", 2);
            Map<String, String> queryParams = this.parseQuery(databaseUrlTokens.length == 2 ? databaseUrlTokens[1] : "");
            File database = new File(databaseUrlTokens[0]);
            if (!database.exists()) {
                queryParams.put("create", "true");
            } else if (queryParams.get("create") != null) {
                queryParams.remove("create");
            }

            // Remove default user/password, if added at config for some reasons
            if (queryParams.get("user") != null) {
                queryParams.remove("user");
            }

            if (queryParams.get("password") != null) {
                queryParams.remove("password");
            }

            if (user != null && !user.isEmpty()) {
                queryParams.put("user", user);
                queryParams.put("password", password != null ? password : "");
            }

            if (queryParams.containsKey("create")) {
                // Create database and close it.
                Logger.getLogger(ApacheDerbyEmbeddedDriver.class.getName()).log(Level.WARNING, "Creating database: {0}", database.getAbsolutePath());
                try {
                    this.connection = DriverManager.getConnection("jdbc:derby:" + database.getAbsolutePath() + this.renderQuery(queryParams));
                    if (this.callback != null) {
                        this.callback.onCreateDatabase();
                    }
                    this.close();
                    queryParams.remove("create");
                    Logger.getLogger(ApacheDerbyEmbeddedDriver.class.getName()).log(Level.INFO, "Database {0} has been created successfully.", databaseUrlTokens[0]);
                } catch (SQLException ex) {
                    Logger.getLogger(ApacheDerbyEmbeddedDriver.class.getName()).log(Level.SEVERE,
                            "Could not create database {0}... :-(", databaseUrlTokens[0]);
                }
            }

            this.connection = DriverManager.getConnection("jdbc:derby:" + databaseUrlTokens[0] + this.renderQuery(queryParams));
            if (this.callback != null) {
                this.callback.onConnect();
            }
        } catch (SQLException ex) {
            Logger.getLogger(ApacheDerbyEmbeddedDriver.class.getName()).log(Level.SEVERE, null, ex);
            throw new Exception("Could not connect to the database.");
        }

        return this;
    }


    /**
     * Parse and patch query.
     *
     * @param params
     * @return
     */
    private Map<String, String> parseQuery(String params) {
        Map<String, String> query = new HashMap<String, String>();
        if (params != null) {
            params = params.trim();
            if (!params.isEmpty()) {
                String[] paramsSets = params.split(";");
                for (int i = 0; i < paramsSets.length; i++) {
                    String[] kv = paramsSets[i].split("=");
                    query.put(kv[0].trim().toLowerCase(), kv[1].trim());
                }
            }
        }

        return query;
    }


    /**
     *
     * @param query
     * @return
     */
    private String renderQuery(Map<String, String> params) {
        StringBuilder query = new StringBuilder();
        for (String key:params.keySet()) {
            query.append(";").append(key).append("=").append(params.get(key));
        }

        return query.toString();
    }
}
