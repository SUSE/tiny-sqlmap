package de.suse.lib.sqlmap;

import java.sql.SQLException;

/**
 *
 * @author bo
 */
public interface ConnectionCallback {
    public void onCreateDatabase() throws SQLException;
    public void onConnect() throws SQLException;
    public void onClose() throws SQLException;
}
