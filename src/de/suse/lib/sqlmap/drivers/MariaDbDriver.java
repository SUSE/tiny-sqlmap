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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.mariadb.jdbc.MySQLDataSource;


/**
 *
 * @author bo
 */
public class MariaDbDriver extends GenericDriver {
    private final static String PROTO = "jdbc:mysql://";
    private boolean useSSL = true;
    private final static String DRIVER = "org.mariadb.jdbc.Driver";
    private MySQLDataSource dataSource;

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


    public MariaDbDriver _connect(String user, String password) throws Exception {
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

    @Override
    public GenericDriver connect(String user, String password) throws Exception {
        if (this.dataSource == null) {
            this.dataSource = new MySQLDataSource();
            this.dataSource.setServerName(this.getHost());
            this.dataSource.setDatabaseName(this.getDatabaseName());
            this.dataSource.setPortNumber(Integer.parseInt(this.getPort()));
            this.dataSource.setUser(user);
            this.dataSource.setPassword(password);
        }

        return this;
    }
    
    

    @Override
    public Connection getConnection() {
        try {
            this.connection = this.dataSource.getConnection();
        } catch (SQLException ex) {
            Logger.getLogger(MariaDbDriver.class.getName()).log(Level.SEVERE, null, ex);
        }

        return this.connection;
    }
}
