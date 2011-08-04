package de.suse.lib.sqlmap.drivers;

import de.suse.lib.sqlmap.ConnectionInfo;
import de.suse.lib.sqlmap.pool.EmbeddedConnectionPool;
import java.net.URISyntaxException;
import java.sql.SQLException;

/**
 *
 * @author bo
 */
public class H2EmbeddedServerDriver extends GenericDriver {
    private final static String PROTO = "jdbc:h2:tcp://";
    private final static String DRIVER = "org.h2.Driver";
    private ConnectionInfo info;
    private EmbeddedConnectionPool connectionPool;


    public H2EmbeddedServerDriver(ConnectionInfo info)
            throws URISyntaxException,
                   SQLException,
                   Exception {
        super(info.getUrl());
        this.info = info;
        Class.forName(H2EmbeddedServerDriver.DRIVER);
    }


    @Override
    protected void parseURL() throws SQLException {}


    private EmbeddedConnectionPool getConnectionPool() {
        if (this.connectionPool == null) {
            this.connectionPool = new EmbeddedConnectionPool(this.getUrl(), this.info.getUser(), this.info.getPassword());
        }

        return this.connectionPool;
    }


    @Override
    @Deprecated
    public H2EmbeddedServerDriver connect(String user, String password) throws Exception {
        return this.connect();
    }


    public H2EmbeddedServerDriver connect() throws SQLException, Exception {
        this.connection = this.getConnectionPool().getConnection();
        return this;
    }

    public int getPoolSize() {
        return this.getConnectionPool().getPoolSize();
    }
}
