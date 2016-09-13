package com.github.jasoma.graft.test

import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import org.junit.rules.ExternalResource
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Transaction
import org.neo4j.test.TestGraphDatabaseFactory

/**
 * Test fixture for creating and disposing of an in-memory neo4j database. A cypher script can be provided as
 * part of construction to seed the database with values.
 */
class InMemoryDatabase extends ExternalResource implements GraphDatabaseService {

    @Delegate private GraphDatabaseService db

    private final String seedQuery

    /**
     * Creates a new empty database.
     */
    InMemoryDatabase() {
        seedQuery = null
    }

    /**
     * Creates a new database and runs the provided seed against it.
     *
     * @param query the seed to execute against the database.
     */
    InMemoryDatabase(String query) {
        seedQuery = query
    }

    /**
     * Creates a new database and runs the provided seed against it.
     *
     * @param querySource a source to read the seed from.
     */
    InMemoryDatabase(Reader querySource) {
        seedQuery = querySource.readLines().join("\n")
    }

    @Override
    protected void before() throws Throwable {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase()
        if (seedQuery) {
            seedDb()
        }
    }

    private void seedDb() {
        def tx = db.beginTx()
        try {
            db.execute(seedQuery).close();
            tx.success()
        }
        finally {
            tx.close()
        }
    }

    @Override
    protected void after() {
        db.shutdown()
    }

    /**
     * Starts a new transaction and then runs the provided closure. The transaction is guaranteed to be closed when
     * the closure exits. The database instance itself is passed to the closure as an argument while the transaction
     * acts as the delegate allowing for easy access to the {@link Transaction#success()} or {@link Transaction#failure()}
     * methods. Example:
     *
     * <pre><code>
     *     db.withTransaction { db ->
     *          def results = db.execute("CREATE (n:TEST)")
     *          // ...
     *          if (condition) {
     *              success()
     *          }
     *          else {
     *              rollback()
     *          }
     *     }
     * </code></pre>
     *
     * @param closure the closure to run with the transaction.
     */
    public def void withTransaction(@DelegatesTo(Transaction)
                                    @ClosureParams(value=SimpleType.class, options="org.neo4j.graphdb.GraphDatabaseService") Closure closure) {
        def tx = db.beginTx()
        closure.delegate = tx
        closure.resolveStrategy = Closure.DELEGATE_FIRST
        try {
            closure(db)
        }
        finally {
            tx.close()
        }
    }
}
