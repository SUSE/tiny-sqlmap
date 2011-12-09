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


/**
 *
 * @author Bo Maryniuk
 */
public class PgSQLDriver extends GenericDriver {
    private final static String PROTO = "jdbc:postgresql://";
    private boolean useSSL = true;
    private final static String DRIVER = "org.postgresql.Driver";


    /**
     * Create a driver to an URL connection.
     * 
     * @param url
     * @throws URISyntaxException
     * @throws ClassNotFoundException
     * @throws SQLException 
     */
    public PgSQLDriver(String url)
            throws URISyntaxException,
                   ClassNotFoundException,
                   SQLException,
                   Exception {
        super(url);
        Class.forName(PgSQLDriver.DRIVER);
    }

    
    /**
     * Create a driver out of the connection.
     * 
     * @param conn 
     */
    public PgSQLDriver(Connection conn) {
        super(conn);
    }


    @Override
    protected void parseURL() throws SQLException {
        if (this.getUrl().startsWith(PgSQLDriver.PROTO)) {
            String host = this.getUrl().replace(PgSQLDriver.PROTO, "").split("/")[0];
            this.setPort(host.contains(":") ? host.split(":")[1] : "5432");
            this.setHost(host.contains(":") ? host.split(":")[0] : host);
            this.setDatabase(this.getUrl().replace(PgSQLDriver.PROTO, "").split("/")[1].replaceAll(" ", ""));
        } else {
            throw new SQLException("URL has no " + this.getClass().getName() + " protocol signature.");
        }
    }

    
    /**
     * Use SSL connection?
     * 
     * @param useSSL 
     */
    public PgSQLDriver setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
        return this;
    }

    
    /**
     * Connect to the PostgreSQL database with already given URL.
     * 
     * @param user
     * @param password 
     */
    @Override
    public PgSQLDriver connect(String user, String password) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        if (this.useSSL) {
            properties.setProperty("ssl", "true"); // In fact, "false" is also "true" here. :-(
        }
        try {
            this.connection = DriverManager.getConnection(PgSQLDriver.PROTO + this.getHost() + ":" + this.getPort() + "/" + this.getDatabaseName(), properties);
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
    public Connection getConnection() {
        return this.connection;
    }
}
