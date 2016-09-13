package com.github.jasoma.graft.test

import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import groovy.util.logging.Slf4j
import org.junit.rules.ExternalResource
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.Session

/**
 * Test fixture for establishing a connection to a neo4j server. Connection details can be provided on construction or
 * be placed on the classpath in a {@code neoserver.properties} file. If configuring via properties the following values
 * are used:
 *
 * <pre><code>
 *      neoserver.url=http://localhost:7474   # url of the server
 *      neoserver.user=user                   # username to authenticate with
 *      neoserver.password=secret             # password to use for the user
 * </code></pre>
 *
 * If no user and password is specified in the properties file the connection will be made with without authentication.
 * When the scope of the connection expires the driver an all open sessions are closed.
 */
@Slf4j
class NeoServerConnection extends ExternalResource implements Driver {

    public static final String URL_PROPERTY = "neoserver.url"
    public static final String USERNAME_PROPERTY = "neoserver.username"
    public static final String PASSWORD_PROPERTY = "neoserver.password"

    private Map properties
    private List<Session> sessions = []

    @Delegate private Driver driver

    /**
     * Creates the connection from a {@code neoserver.properties} file found on the classpath.
     */
    NeoServerConnection() {
        def properties = new Properties()
        def stream = getClass().getClassLoader().getResourceAsStream("neoserver.properties")
        if (stream == null) {
            throw new FileNotFoundException("Could not locate 'neoserver.properties' on the classpath")
        }
        stream.withStream { s -> properties.load(s) }
        this.properties = properties
    }

    /**
     * Creates the connection from properties loaded elsewhere.
     *
     * @param properties the properties object to get url, user, and password from.
     */
    NeoServerConnection(Properties properties) {
        this.properties = properties
    }

    /**
     * Create the connection directly.
     *
     * @param url the address of the server.
     * @param username the username to connect as.
     * @param password the password to use.
     */
    NeoServerConnection(String url, String username = null, String password = null) {
        this.properties = [(URL_PROPERTY): url, (USERNAME_PROPERTY): username, (PASSWORD_PROPERTY): password]
    }

    @Override
    protected void before() throws Throwable {
        String url = properties[URL_PROPERTY]
        String username = properties[USERNAME_PROPERTY]
        String password = properties[PASSWORD_PROPERTY]
        if (username && password) {
            driver = GraphDatabase.driver(url, AuthTokens.basic(username, password))
        }
        else if (!username && !password) {
            driver = GraphDatabase.driver(url)
        }
        else {
            def message = username ? "username but no password supplied" : "password but no username supplied"
            throw new IllegalArgumentException(message)
        }
    }

    @Override
    protected void after() {
        sessions.each { s ->
            if (s.isOpen()) {
                log.warn("A session was still open after testing completed, closing it...")
                s.close()
            }
        }
        driver.close()
    }

    /**
     * Establish a session
     * @return a session that could be used to run {@link Session#run(String) a statement} or
     * {@link Session#beginTransaction() a transaction }.
     */
    def Session session() {
        def session = driver.session()
        sessions.add(session)
        return session
    }

    /**
     * Creates a new session and passes it to the provided closure. Session is guaranteed to be
     * closed when the closure exits. Example:
     *
     * <pre><code>
     * Driver driver = ...
     * driver.withSession { session ->
     *     ...
     * }
     * </code></pre>
     *
     * @param closure the closure to run with the session.
     */
    def void withSession(@ClosureParams(value=SimpleType.class, options="org.neo4j.driver.v1.Session") Closure closure) {
        def session = session()
        try {
            closure(session)
        }
        finally {
            session.close()
        }
    }
}
