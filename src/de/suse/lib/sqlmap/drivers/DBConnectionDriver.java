package de.suse.lib.sqlmap.drivers;

import de.suse.lib.sqlmap.ConnectionCallback;
import java.sql.Connection;
import java.sql.SQLException;

/**
 *
 * @author bo
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
}
