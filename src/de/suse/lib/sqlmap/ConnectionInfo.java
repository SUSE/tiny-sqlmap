package de.suse.lib.sqlmap;

/**
 *
 * @author bo
 */
public class ConnectionInfo {
    private String tag;
    private String url;
    private String password;
    private String user;

    public ConnectionInfo(String tag, String url, String user, String password) {
        this.tag = tag;
        this.url = url;
        this.password = password;
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public String getTag() {
        return tag;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getVendor() {
        return this.getUrl().split("://")[0].replace("jdbc:", "").toLowerCase();
    }
}
