/*
 * Driver for embedded H2.
 */

package de.suse.lib.sqlmap.drivers;

import de.suse.lib.sqlmap.ConnectionInfo;
import java.net.URISyntaxException;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author bo
 */
public class H2EmbeddedDriver extends GenericDriver {
    private final static String PROTO = "jdbc:h2://";
    private final static String DRIVER = "org.h2.Driver";
    private ConnectionInfo info;

    public H2EmbeddedDriver(ConnectionInfo info)
            throws URISyntaxException,
                   SQLException,
                   Exception {
        super(info.getUrl());
        this.info = info;
        Class.forName(H2EmbeddedDriver.DRIVER);
    }


    @Override
    protected void parseURL() throws SQLException {
        if (this.getUrl().startsWith(H2EmbeddedDriver.PROTO)) {
            String host = this.getUrl().replace(H2EmbeddedDriver.PROTO, "").split("/")[0]; // "-" is for embedded.
            if (host.equals("-")) {
                this.setDatabase(this.getUrl().replace(H2EmbeddedDriver.PROTO, "").split("-", 0)[1]);
            } else {
                throw new SQLException("H2 supported only in the embedded mode.");
            }
        } else {
            throw new SQLException("URL has no " + this.getClass().getName() + " protocol signature.");
        }
    }


    @Override
    public H2EmbeddedDriver connect(String user, String password) throws Exception {
        if (this.getDatabase() == null || this.getDatabase().isEmpty()) {
            throw new Exception("Unknown database.");
        }

        this.connection = DriverManager.getConnection("jdbc:h2:" + this.getDatabase(), this.info.getUser(), this.info.getPassword());
        return this;
    }
}
