package de.suse.lib.sqlmap;

import de.suse.lib.sqlmap.drivers.H2EmbeddedServerDriver;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.h2.tools.Server;


/**
 *
 * @author bo
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        Thread process = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.err.println("Starting server");
                    Server server = Server.createTcpServer(new String[]{"-tcpPort", 8181 + ""}).start();
                    System.err.println("Done: " + server);
                } catch (Exception ex) {
                    System.err.println("Error starting server: " + ex.getLocalizedMessage());
                }
            }
        });

//        process.setDaemon(false);
        process.start();

        ConnectionInfo nfo = new ConnectionInfo("h2", "jdbc:h2:tcp://localhost:8181/bullshit", "sa", "");
        final H2EmbeddedServerDriver h2ed = new H2EmbeddedServerDriver(nfo);
        h2ed.connect();


        for (int i = 0; i < 100; i++) {
            Thread connProcess = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < 100; j++) {
                        Connection conn = h2ed.getConnection();
                        try {
                            Statement statement = conn.createStatement();
                            ResultSet result = statement.executeQuery("SELECT now() as X");
                            if (result != null) {
                                result.next();
                                System.err.println(result.getTimestamp("X"));
                                result.clearWarnings();
                                result.close();
                            }
                            Thread.sleep(100);
                            conn.clearWarnings();
                            conn.close();

                        } catch (Exception ex) {
                            System.err.println("Ouch: " + ex.getLocalizedMessage());
                        }
                    }
                }
            });
            connProcess.setDaemon(true);
            connProcess.start();
            System.err.println("> " + connProcess.getName());
        }

        Thread.currentThread().join();
    }
}
