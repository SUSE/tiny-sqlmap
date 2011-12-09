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

import de.suse.lib.sqlmap.ConnectionCallback;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base driver operator.
 * 
 * @author Bo Maryniuk
 */
public abstract class GenericDriver implements DBConnectionDriver {
    private String host;
    private String database;
    private String port; // We don't need Integer in here to wrap it back to the URL.
    private String url;
    protected Connection connection;
    protected ConnectionCallback callback;

    GenericDriver() {}

    
    /**
     * Create connection directly from already opened connection.
     * 
     * @param conn 
     */
    public GenericDriver(Connection conn) {
        this.connection = conn;
    }


    /**
     * Create a connection from an URL.
     * 
     * @param url
     * @throws URISyntaxException
     * @throws SQLException 
     */
    public GenericDriver(String url) 
            throws URISyntaxException,
                   SQLException,
                   Exception {
        if (url == null || url.trim().equals("")) {
            throw new Exception("Syntax error: URL can not be empty.");
        }
        
        url = url.replaceAll(" ", "");

        if (url.split("://").length != 2) {
            throw new URISyntaxException(url, "Syntax error: URL should have proto and driver name defined.");
        }

        if (url.split("://")[1].split("/").length < 2) {
            throw new URISyntaxException(url, "Syntax error: URL should have both host and a database defined.");
        }
        
        this.url = url;
        this.parseURL();
    }
    
    
    /**
     * Parse URL for connect.
     */
    protected void parseURL() throws SQLException {
        throw new UnsupportedOperationException("Parsing is not yet implemented for this driver.");
    }

    
    /**
     * Connect to the database.
     */
    @Override
    public GenericDriver connect(String user, String password) throws Exception {
        throw new UnsupportedOperationException("Parsing is not yet implemented for this driver.");        
    }


    /**
     * Check if driver is connected at the moment.
     * @return 
     */
    @Override
    public boolean isConnected() {
        try {
            return this.connection != null && !this.connection.isClosed();
        } catch (SQLException ex) {
            Logger.getLogger(GenericDriver.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return false;
    }
    

    @Override
    public String getDatabaseName() {
        return database;
    }

    protected void setDatabase(String database) {
        this.database = database;
    }


    protected String getHost() {
        return host;
    }
    
    protected void setHost(String host) {
        this.host = host;
    }

    
    protected String getPort() {
        return port;
    }

    protected void setPort(String port) {
        this.port = port;
    }
    
    protected String getUrl() {
        return url;
    }

    @Override
    public void close() {
        if (this.callback != null) {
            try {
                this.callback.onClose();
            } catch (SQLException ex) {
                Logger.getLogger(PgSQLDriver.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        if (this.connection != null) {
            try {
                this.connection.clearWarnings();
            } catch (SQLException ex) {
                Logger.getLogger(GenericDriver.class.getName()).log(Level.SEVERE, null, ex);
            }

            try {
                this.connection.close();
            } catch (SQLException ex) {
                Logger.getLogger(GenericDriver.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    @Override
    public Connection getConnection() {
        return this.connection;
    }


    @Override
    public void setConnectionCallback(ConnectionCallback callback) {
        this.callback = callback;
    }

}
