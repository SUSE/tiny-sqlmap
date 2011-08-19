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

import de.suse.lib.sqlmap.drivers.H2EmbeddedDriver;
import de.suse.lib.sqlmap.drivers.ApacheDerbyEmbeddedDriver;
import de.suse.lib.sqlmap.drivers.DBConnectionDriver;
import de.suse.lib.sqlmap.drivers.H2EmbeddedServerDriver;
import de.suse.lib.sqlmap.drivers.PgSQLDriver;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * SQL Mapper
 *
 * @author Bo Maryniuk
 */
public class SQLMapper {
    private static final String DB_VENDOR_POSTGRESQL = "postgresl";
    private static final String DB_VENDOR_ORACLE = "oracle";
    private static final String DB_VENDOR_H2 = "h2";

    private Map<String, ConnectionInfo> connectionInfo;
    private DBConnectionDriver connectionDriver;
    private Class resourcesClass;
    private ConnectionCallback connectionCallback;
    private boolean debug = false;
    private String tag;
    
    
    private void init(Properties config) throws SQLException {
        this.connectionInfo = new HashMap<String, ConnectionInfo>();
        this.parseConnectionInfo(config);
        this.connectionCallback = null;
    }


    /**
     * Create an SQL Mapper from the properties.
     * 
     * @param config
     * @throws SQLException 
     */
    public SQLMapper(Properties config) throws SQLException {
        this.init(config);
    }


    /**
     * Create an SQL Mapper out of the already opened connection.
     * 
     * @param connection
     * @throws SQLException 
     */
    public SQLMapper(Connection connection) throws SQLException {
        //this.connectionDriver
        throw new NotImplementedException();
    }

    
    /**
     * Create an SQL Mapper on top of JNDI connection.
     * 
     * @param jndiName
     * @throws SQLException 
     */
    public SQLMapper(String jndiName) 
            throws SQLException,
                   NamingException {
        Connection conn = (Connection) new InitialContext().lookup(jndiName);
        if (conn.getMetaData().getURL().toLowerCase().split("://")[0].endsWith(SQLMapper.DB_VENDOR_POSTGRESQL)) {
            this.connectionDriver = new PgSQLDriver(conn);
        }
    }


    public SQLMapper(File configFile) 
            throws FileNotFoundException,
                   IOException,
                   SQLException {
        Properties props = new Properties();
        props.load(new FileInputStream(configFile));
        this.init(props);
    }


    public void setDebug(boolean debug) {
        this.debug = debug;
    }
    
    

    /**
     * Connect to the database using a tag.
     * 
     * @param tag 
     */
    public SQLMapper connect(String tag) 
            throws URISyntaxException,
                   ClassNotFoundException,
                   Exception {
        this.close();
        if (tag != null) {
            this.tag = tag;
        }

        if (this.tag == null) {
            throw new Exception("Don't known what connect to: the tag is empty.");
        }

        ConnectionInfo info = this.connectionInfo.get(this.tag);
        if (info.getVendor().equals("postgresql")) {
            this.connectionDriver = new PgSQLDriver(info.getUrl()).setUseSSL(false).connect(info.getUser(), info.getPassword());
        } else if (info.getVendor().equals("derby")) {
            System.err.println("INFO: " + info.getUrl());
            this.connectionDriver = new ApacheDerbyEmbeddedDriver(info);
        } else if (info.getVendor().equals("h2")) {
            System.err.println("INFO: " + info.getUrl());
            this.connectionDriver = new H2EmbeddedDriver(info);
        } else if (info.getVendor().equals("h2:tcp")) {
            System.err.println("INFO: " + info.getUrl());
            this.connectionDriver = new H2EmbeddedServerDriver(info);
        } else {
            throw new Exception(String.format("Vendor \"%s\" is not supported.", info.getVendor()));
        }

        this.connectionDriver.setConnectionCallback(this.connectionCallback);
        this.connectionDriver.connect(info.getUser(), info.getPassword());

        System.err.println("Connection: " + this.connectionDriver.getConnection());
        String db = this.connectionDriver.getConnection().getMetaData().getDatabaseProductName();
        String url = this.connectionDriver.getConnection().getMetaData().getURL();
        System.err.println("DB: " + db + ", URL: " + url);

        return this;
    }
    

    /**
     * Parse connection information from the Properties.
     * 
     * @param config 
     */
    private void parseConnectionInfo(Properties config) throws SQLException {
        String dbTags = config.getProperty(de.suse.lib.sqlmap.Constants.DB_TAGS); 
        if (dbTags == null) {
            throw new SQLException("Can not find databases tag in the configuration.");
        } else {
            dbTags = dbTags.replaceAll(" ", "");
        }
        
        String[] tags = dbTags.split(",");
        for (int i = 0; i < tags.length; i++) {
            String tag = tags[i].trim();
            if (tag.equals("")) {
                continue;
            }
            
            ConnectionInfo info = new ConnectionInfo(tag,
                    config.getProperty(tag + "." + de.suse.lib.sqlmap.Constants.DB_TAG_URL),
                    config.getProperty(tag + "." + de.suse.lib.sqlmap.Constants.DB_TAG_USER),
                    config.getProperty(tag + "." + de.suse.lib.sqlmap.Constants.DB_TAG_PASSWORD));
            if (info.getUser() != null && info.getUrl() != null) {
                this.connectionInfo.put(tag, info);
            }
        }
    }


    /**
     * Call SQL query. ResultSet will be set to null in case it is not a SELECT statement.
     * 
     * @param querypath
     * @param params
     * @return
     * @throws SQLIntegrityConstraintViolationException
     * @throws SQLException
     * @throws Exception
     */
    public ResultSet call(String querypath, Map<?, ?> params)
            throws SQLIntegrityConstraintViolationException,
                   SQLException,
                   Exception {
        if (this.connectionDriver == null || !this.connectionDriver.isConnected()) {
            this.connect(null); // reuse last tag
        }

        ResultSet result = null;
        String[] queryTemplates = this.getTemplateFromResource(querypath).split(";");
        
        // Check template conventions
        if (queryTemplates.length == 0) {
            throw new Exception("None of SQL statements has been found in the given SQL template.");
        } else if (queryTemplates.length > 1) {
            int selective = 0;
            for (int i = 0; i < queryTemplates.length; i++) {
                if (queryTemplates[i].toLowerCase().startsWith("select")) {
                    selective++;
                    if (selective > 1) {
                        throw new Exception("There can be only one SELECT statement in a multi-statement query.");
                    }
                }
            }
        }

        // Process operations
        boolean commit = false;
        for (int i = 0; i < queryTemplates.length; i++) {
            if (this.debug) {
                System.err.println("Processing statement:");
                System.err.println("-----");
                System.err.println(queryTemplates[i]);
                System.err.println("-----");
            }

            PreparedStatement statement = null;
            try {
                statement = new SQLParser(queryTemplates[i]).parse(params).prepare(this.connectionDriver.getConnection());
            } catch (SQLIntegrityConstraintViolationException ex) {
                if (this.debug) {
                    Logger.getLogger(SQLMapper.class.getName()).log(Level.WARNING, "Integrity error while preparing: {0}", ex.getLocalizedMessage());
                }
                this.rollback();
                throw new SQLIntegrityConstraintViolationException(ex);
            } catch (SQLException ex) {
                Logger.getLogger(SQLMapper.class.getName()).log(Level.SEVERE, "SQL error while preparing: {0}", ex.getLocalizedMessage());
                this.rollback();
                throw new SQLException(ex);
            } catch (Exception ex) {
                Logger.getLogger(SQLMapper.class.getName()).log(Level.SEVERE, "Unhlandled error while preparing: {0}", ex.getLocalizedMessage());
                this.rollback();
                throw new Exception(ex);
            }

            // Panic! Entire chain failed.
            if (statement == null) {
                return null;
            }

            // Execute operations
            try {
                if (statement.execute()) {
                    result = statement.getResultSet();
                } else {
                    commit = true;
                }
            } catch (SQLIntegrityConstraintViolationException ex) {
                Logger.getLogger(SQLMapper.class.getName()).log(Level.SEVERE, "Integrity error: {0}", ex.getLocalizedMessage());
                this.rollback();
                throw new SQLIntegrityConstraintViolationException(ex);
            } catch (SQLException ex) {
                Logger.getLogger(SQLMapper.class.getName()).log(Level.SEVERE, "SQL error: {0}", ex.getLocalizedMessage());
                this.rollback();
                throw new SQLException(ex);
            } catch (Exception ex) {
                Logger.getLogger(SQLMapper.class.getName()).log(Level.SEVERE, "Unhandled error: {0}", ex.getLocalizedMessage());
                this.rollback();
                throw new Exception(ex);
            }
        }

        // Commit data, if required.
        if (commit) {
            this.commit();
        }

        return result;
    }
    

    /**
     * Commit the transaction.
     */
    private void commit() {
        try {
            this.connectionDriver.getConnection().commit();
        } catch (SQLException ex) {
            Logger.getLogger(SQLMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    

    /**
     * Rollback transaction.
     */
    private void rollback() {
        try {
            this.connectionDriver.getConnection().rollback();
        } catch (SQLException ex1) {
            Logger.getLogger(SQLMapper.class.getName()).log(Level.SEVERE, null, ex1);
        }
    }
    
    
    /**
     * Close connection to the database and result.
     */
    public void close(ResultSet result) {
        if (result != null) {
            try {
                result.clearWarnings();
                result.close();
            } catch (SQLException ex) {
                Logger.getLogger(SQLMapper.class.getName()).log(Level.WARNING, null, ex);
            } catch (Exception ex) {
                Logger.getLogger(SQLMapper.class.getName()).log(Level.SEVERE, null, ex);                
            } finally {
                result = null;
            }
        }

        this.close();
    }
    
    
    /**
     * Close connection to the database.
     */
    public void close() {
        if (this.connectionDriver != null && this.connectionDriver.isConnected()) {
            this.connectionDriver.close();
        }
    }

    
    /**
     * Read SQL template from the namespace.
     * 
     * @param resource
     * @return 
     */
    private String getTemplateFromResource(String resource) {
        if (resource == null) {
            return null;
        } else if (resource.toLowerCase().startsWith("file://")) {
            System.err.println("Not yet implemented.");
        } else if (this.resourcesClass != null) {
            return this.getTemplateFromResource(this.resourcesClass.getResourceAsStream("/" + resource.replaceAll("\\.", "/") + ".tsql"));
        }
        
        return null;
    }


    private String getTemplateFromResource(InputStream inputStream) {
        BufferedReader bufferedReader = null;
        String line;
        StringBuilder buff = new StringBuilder();

        try {
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            while (null != (line = bufferedReader.readLine())) {
                buff.append(line).append("\n");
            }
        } catch (Exception ex) {
            Logger.getLogger(SQLMapper.class.getName()).log(Level.WARNING, null, ex);
        } finally {
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }

                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException ex) {
                Logger.getLogger(SQLMapper.class.getName()).log(Level.WARNING, null, ex);
            }
        }

        return buff.toString();
    }

    
    /**
     * Set resources class.
     * 
     * @param cls
     * @return 
     */
    public SQLMapper setResourceClass(Class cls) {
        this.resourcesClass = cls;
        return this;
    }


    public SQLMapper setInitCallback(ConnectionCallback callback) {
        this.connectionCallback = callback;
        return this;
    }
}
