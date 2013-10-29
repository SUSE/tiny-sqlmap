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
import java.sql.Connection;
import java.sql.DatabaseMetaData;

/**
 * @author Bo Maryniuk
 */
public interface DBConnectionDriver {
    /**
     * Connect to the database.
     * @param user
     * @param password 
     * @return 
     * @throws Exception 
     * @deprecated
     */
    public DBConnectionDriver connect(String user, String password) throws Exception;

    /**
     * Connect to the database.
     *
     * @return
     * @throws SQLException
     * @throws Exception
     */
    //public DBConnectionDriver connect() throws SQLException, Exception;

    /**
     * Check if driver is connected at the moment.
     * @return 
     */
    public boolean isConnected();

    public void close();
    
    public Connection getConnection();

    public void setConnectionCallback(ConnectionCallback callback);

    public String getDatabaseName();
    
    public DatabaseMetaData getDatabaseMetaData();
}
