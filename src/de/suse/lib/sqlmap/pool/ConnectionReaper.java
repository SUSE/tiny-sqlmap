package de.suse.lib.sqlmap.pool;

/**
 *
 * @author bo
 */
public class ConnectionReaper  extends Thread {
    private EmbeddedConnectionPool pool;
    private final long delay = 300000;

    ConnectionReaper(EmbeddedConnectionPool pool) {
        this.pool = pool;
    }


    public void run() {
        while (true) {
            try {
                Thread.sleep(ConnectionReaper.this.delay);
            } catch (InterruptedException e) {

            }

            pool.reapConnections();
        }
    }
}
