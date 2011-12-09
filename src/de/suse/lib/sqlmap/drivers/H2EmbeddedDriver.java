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

import de.suse.lib.sqlmap.ConnectionInfo;
import java.net.URISyntaxException;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Driver operator for the H2 database in embedded mode.
 *
 * @author Bo Maryniuk
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
        if (this.getDatabaseName() == null || this.getDatabaseName().isEmpty()) {
            throw new Exception("Unknown database.");
        }

        this.connection = DriverManager.getConnection("jdbc:h2:" + this.getDatabaseName(), this.info.getUser(), this.info.getPassword());
        return this;
    }
}
