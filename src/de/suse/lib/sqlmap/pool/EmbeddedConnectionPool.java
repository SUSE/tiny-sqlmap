package de.suse.lib.sqlmap.pool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Connection pool for embedded purposes.
 *
 * @author bo
 */
public class EmbeddedConnectionPool {
    private List<EmbeddedConnection> connections;
    private String url, user, password;
    final private long timeout = 60000;
    private ConnectionReaper reaper;
    final private int poolsize = 10;


    public EmbeddedConnectionPool(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.connections = new ArrayList<EmbeddedConnection>();
        reaper = new ConnectionReaper(this);
        reaper.start();
    }


    public synchronized void reapConnections() {
        long stale = System.currentTimeMillis() - timeout;

        for (int i = 0; i < connections.size(); i++) {
            EmbeddedConnection conn = connections.get(i);
            if ((conn.inUse()) && (stale > conn.getLastUse()) && (!conn.validate())) {
                this.removeConnection(conn);
            }
        }
    }


    public synchronized void closeConnections() {
        for (int i = 0; i < connections.size(); i++) {
            removeConnection(connections.get(i));
        }
    }


    private synchronized void removeConnection(EmbeddedConnection conn) {
        connections.remove(conn);
    }


    public synchronized EmbeddedConnection getConnection() throws SQLException {
        for (int i = 0; i < connections.size(); i++) {
            EmbeddedConnection conn = connections.get(i);

            if (conn.lease()) {
                return conn;
            }
        }

        Connection conn = DriverManager.getConnection(url, user, password);
        EmbeddedConnection embeddedConnection = new EmbeddedConnection(conn, this);
        embeddedConnection.lease();
        connections.add(embeddedConnection);
        return embeddedConnection;
    }


    /**
     * 
     * @param conn
     */
    public synchronized void returnConnection(EmbeddedConnection conn) {
        conn.expireLease();
    }

    public int getPoolSize() {
        return this.connections.size();
    }
}
