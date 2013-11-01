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

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

/**
 * Export SQLMapper object through JNDI.
 * 
 * Example usage in Tomcat:
 * 
 * Add to server.xml within <context> tag the following:
 * 
 *   <Resource name="tiny/sqlmap"
 *             auth="Container"
 *             type="de.suse.lib.sqlmap.SQLMapper"
 *             factory="de.suse.lib.sqlmap.SQLMapperJNDIFactory"
 * 
 *             db_url="jdbc:mysql://yourhost.com/yourdatabasename"
 *             db_user="scott"
 *             db_password="tiger"
 *   />
 * 
 * @author bo
 */
public class SQLMapperJNDIFactory implements ObjectFactory {
    // Params to pass from the JNDI container.
    public static final String ARG_DB_URL = "db_url";
    public static final String ARG_DB_USER = "db_user";
    public static final String ARG_DB_PASSWORD = "db_password";

    private static SQLMapper mapperInstance;

    @Override
    public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable<?, ?> environment) throws Exception {
        synchronized(this) {
            if (SQLMapperJNDIFactory.mapperInstance == null) {
                Reference ref = (Reference) obj;
                Enumeration addrs = ref.getAll();

                Properties mapperSetup = new Properties();
                mapperSetup.put("databases", "sqldb");
                
                while (addrs.hasMoreElements()) {
                    RefAddr addr = (RefAddr) addrs.nextElement();
                    String arg = addr.getType();
                    String value = (String) addr.getContent();
                    if (arg.equals(SQLMapperJNDIFactory.ARG_DB_URL)) {
                        mapperSetup.put("sqldb.url", value);
                    } else if (arg.equals(SQLMapperJNDIFactory.ARG_DB_USER)) {
                        mapperSetup.put("sqldb.user", value);
                    } else if (arg.equals(SQLMapperJNDIFactory.ARG_DB_PASSWORD)) {
                        mapperSetup.put("sqldb.password", value);
                    }
                }

                SQLMapperJNDIFactory.mapperInstance = new SQLMapper(mapperSetup);
            }
        }

        return SQLMapperJNDIFactory.mapperInstance;
    }
}
