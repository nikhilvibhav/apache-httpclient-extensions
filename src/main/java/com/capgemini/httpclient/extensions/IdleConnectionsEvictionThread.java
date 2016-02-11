package com.capgemini.httpclient.extensions;

import com.netflix.config.DynamicPropertyFactory;
import org.apache.http.conn.HttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Thread to periodically check the connection pool for idle and expired
 * connections and close them to free up the system resources.
 * <p>
 * Connections sitting around idle in the HTTP connection pool for too long will
 * usually be terminated by the server end of the connection, and will go into
 * CLOSE_WAIT on the client side. If this happens, sockets will sit around in
 * CLOSE_WAIT, still using resources on the client side to manage that socket.
 * Many sockets stuck in CLOSE_WAIT can prevent the OS from creating new
 * connections.
 * <p>
 * This class closes idle connections before they can move into the CLOSE_WAIT
 * state.
 *
 * @author Ganga Aloori
 */
public final class IdleConnectionsEvictionThread extends Thread {

    //Default interval(in milliseconds) between the idle connection eviction thread executions
    private final int EVICTION_INTERVAL = 1000 * 60 * 10;

    //Deafult maximum time(in milliseconds) that a connection can stay idle in the pool
    private final int MAX_IDLE_TIME = 1000 * 60 * 10;

    //Connection manager whose connections will be periodically checked
    private final HttpClientConnectionManager connMgr;

    //Set to true to shutdown the eviction thread
    private volatile boolean shutdown;

    private static final Logger LOGGER = LoggerFactory.getLogger(IdleConnectionsEvictionThread.class);

    public IdleConnectionsEvictionThread(HttpClientConnectionManager connMgr) {
        super("idle-connections-eviction-thread");
        this.connMgr = connMgr;
    }

    @Override
    public void run() {
        DynamicPropertyFactory propertyFactory = DynamicPropertyFactory.getInstance();
        int evictionInterval = propertyFactory.getIntProperty("http.connections.evictionInterval", EVICTION_INTERVAL).getValue();
        int maxIdleTime = propertyFactory.getIntProperty("http.connections.maxIdleTime", MAX_IDLE_TIME).getValue();
        try {
            while (!shutdown) {
                synchronized (this) {
                    LOGGER.debug("Closing the expired and idle connections from the pool.");
                    wait(evictionInterval);
                    connMgr.closeExpiredConnections();
                    connMgr.closeIdleConnections(maxIdleTime, TimeUnit.MILLISECONDS);
                }
            }
        } catch (InterruptedException ex) {
            LOGGER.warn("Unable to close expired and idle connections.", ex);
        }
    }

    /**
     * Shuts down the thread
     */
    public void shutdown() {
        shutdown = true;
        synchronized (this) {
            notifyAll();
        }
        LOGGER.debug("Shutdown idle connections eviction thread.");
    }
}
