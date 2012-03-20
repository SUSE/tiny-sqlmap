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

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLClientInfoException;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author Bo Maryniuk
 */
class EmbeddedConnection implements Connection {

    private EmbeddedConnectionPool pool;
    private Connection conn;
    private boolean inuse;
    private long timestamp;

    public EmbeddedConnection(Connection conn, EmbeddedConnectionPool pool) {
        this.conn = conn;
        this.pool = pool;
        this.inuse = false;
        this.timestamp = 0;
    }

    public synchronized boolean lease() {
        if (inuse) {
            return false;
        } else {
            inuse = true;
            timestamp = System.currentTimeMillis();
            return true;
        }
    }

    public boolean validate() {
        try {
            conn.getMetaData();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public boolean inUse() {
        return inuse;
    }

    public long getLastUse() {
        return timestamp;
    }

    @Override
    public void close() throws SQLException {
        pool.returnConnection(this);
    }

    protected void expireLease() {
        inuse = false;
    }

    protected Connection getConnection() {
        return conn;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return conn.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return conn.prepareCall(sql);
    }

    @Override
    public Statement createStatement() throws SQLException {
        return conn.createStatement();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return conn.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        conn.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return conn.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        conn.commit();
    }

    @Override
    public void rollback() throws SQLException {
        conn.rollback();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return conn.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return conn.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        conn.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return conn.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        conn.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return conn.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        conn.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return conn.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return conn.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        conn.clearWarnings();
    }

    @Override
    public Statement createStatement(int i, int i1) throws SQLException {
        return this.conn.createStatement(i, i1);
    }

    @Override
    public PreparedStatement prepareStatement(String string, int i, int i1) throws SQLException {
        return this.conn.prepareCall(string, i, i1, i1);
    }

    @Override
    public CallableStatement prepareCall(String string, int i, int i1) throws SQLException {
        return this.conn.prepareCall(string, i, i1, i1);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return this.conn.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        this.conn.setTypeMap(map);
    }

    @Override
    public void setHoldability(int i) throws SQLException {
        this.conn.setHoldability(i);
    }

    @Override
    public int getHoldability() throws SQLException {
        return this.conn.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return this.conn.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String string) throws SQLException {
        return this.conn.setSavepoint(string);
    }

    @Override
    public void rollback(Savepoint svpnt) throws SQLException {
        this.conn.rollback(svpnt);
    }

    @Override
    public void releaseSavepoint(Savepoint svpnt) throws SQLException {
        this.conn.releaseSavepoint(svpnt);
    }

    @Override
    public Statement createStatement(int i, int i1, int i2) throws SQLException {
        return this.conn.createStatement(i, i1, i2);
    }

    @Override
    public PreparedStatement prepareStatement(String string, int i, int i1, int i2) throws SQLException {
        return this.conn.prepareCall(string, i, i1, i2);
    }

    @Override
    public CallableStatement prepareCall(String string, int i, int i1, int i2) throws SQLException {
        return this.conn.prepareCall(string, i, i1, i2);
    }

    @Override
    public PreparedStatement prepareStatement(String string, int i) throws SQLException {
        return this.conn.prepareStatement(string, i);
    }

    @Override
    public PreparedStatement prepareStatement(String string, int[] ints) throws SQLException {
        return this.conn.prepareStatement(string, ints);
    }

    @Override
    public PreparedStatement prepareStatement(String string, String[] strings) throws SQLException {
        return this.conn.prepareStatement(string, strings);
    }

    @Override
    public Clob createClob() throws SQLException {
        return this.conn.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return this.conn.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return this.conn.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return this.conn.createSQLXML();
    }

    @Override
    public boolean isValid(int i) throws SQLException {
        return this.conn.isValid(i);
    }

    @Override
    public void setClientInfo(String string, String string1) throws SQLClientInfoException {
        this.conn.setClientInfo(string, string1);
    }

    @Override
    public void setClientInfo(Properties prprts) throws SQLClientInfoException {
        this.conn.setClientInfo(prprts);
    }

    @Override
    public String getClientInfo(String string) throws SQLException {
        return this.conn.getClientInfo(string);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return this.conn.getClientInfo();
    }

    @Override
    public Array createArrayOf(String string, Object[] os) throws SQLException {
        return this.conn.createArrayOf(string, os);
    }

    @Override
    public Struct createStruct(String string, Object[] os) throws SQLException {
        return this.conn.createStruct(string, os);
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        return this.conn.unwrap(type);
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
        return this.conn.isWrapperFor(type);
    }
}
