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
import de.suse.lib.sqlmap.pool.EmbeddedConnectionPool;
import java.net.URISyntaxException;
import java.sql.SQLException;

/**
 *
 * @author Bo Maryniuk
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
