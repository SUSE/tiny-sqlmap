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

package de.suse.lib.sqlmap.pool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Connection pool for embedded purposes.
 *
 * @author Bo Maryniuk
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
