package de.suse.lib.sqlmap.drivers;

import de.suse.lib.sqlmap.ConnectionCallback;
import java.sql.Connection;

/**
 *
 * @author bo
 */
public interface DBConnectionDriver {
    /**
     * Connect to the database.
     */
    public DBConnectionDriver connect(String user, String password) throws Exception;

    /**
     * Check if driver is connected at the moment.
     * @return 
     */
    public boolean isConnected();

    public void close();
    
    public Connection getConnection();

    public void setConnectionCallback(ConnectionCallback callback);
}
