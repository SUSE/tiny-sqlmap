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

import java.net.URISyntaxException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author bo
 */
public class MariaDbDriver extends GenericDriver {
    private final static String PROTO = "jdbc:mysql://";
    private boolean useSSL = true;
    private final static String DRIVER = "org.mariadb.jdbc.Driver";

    public MariaDbDriver(String url) 
            throws URISyntaxException,
                   SQLException,
                   Exception {
        super(url);
        Class.forName(MariaDbDriver.DRIVER);
    }


    @Override
    protected void parseURL() throws SQLException {
        if (this.getUrl().startsWith(MariaDbDriver.PROTO)) {
            String host = this.getUrl().replace(MariaDbDriver.PROTO, "").split("/")[0];
            this.setPort(host.contains(":") ? host.split(":")[1] : "3306");
            this.setHost(host.contains(":") ? host.split(":")[0] : host);
            this.setDatabase(this.getUrl().replace(MariaDbDriver.PROTO, "").split("/")[1].replaceAll(" ", ""));
        } else {
            throw new SQLException("URL has no " + this.getClass().getName() + " protocol signature.");
        }
    }


    @Override
    public MariaDbDriver connect(String user, String password) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        
        try {
            this.connection = DriverManager.getConnection(MariaDbDriver.PROTO + this.getHost() 
                                                          + ":" + this.getPort() 
                                                          + "/" + this.getDatabaseName(), properties);
            if (this.callback != null) {
                this.callback.onConnect();
            }
        } catch (SQLException ex) {
            Logger.getLogger(PgSQLDriver.class.getName()).log(Level.SEVERE, null, ex);
            throw new Exception("Could not connect to the PostgreSQL database.");
        }
        
        return this;
    }

}
