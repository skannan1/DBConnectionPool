/**
 * 
 */
package db.pool;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;


/**
 * This class is used to acquire a database connection from the connection pool
 * and hold it until the session is ended.
 * 
 * After <code>DBSession.begin()</code> is invoked, any call from DAO classes to
 * get a connection for the same <code>dataSourceName</code> will retrieve the
 * connection from the initiated database session.
 * 
 * After database session is initiated, invoking <code>Connection.close()</code>
 * method from DAO class or anywhere will not cause the connection to be
 * returned to the connection pool. The database connection in use on the
 * database session will only be returned to the connection pool when
 * <code>DBSession.close()</code> is invoked.
 * 
 * The usage of this class is strongly recommended when the component has to
 * perform database write operations on multiple tables from the same database
 * and there is one DAO class implemented for each or some of those operations.
 * 
 * Nested database session is not supported and not allowed.
 * 
 * The database connection in use by this session will always work in non auto
 * commit mode (autocommit = false). If the pool has database connection in
 * autocommit mode on, the connection acquired will be turned off by this
 * session, and turned back on when returned to the pool (session ended).
 * 
 * Therefore, once the database connection is associated to the session,
 * tentative to change the autocommit mode of the connection will cause
 * <code>DatabaseSystem</code> exception.
 * 
 * This class is not thread safe.
 * 
 * Suggested usage is: <code>
 *  - on your Handler class implementation or at the point you need 
 *  to start the database transaction do:
 *  
 *  ...
 *  DBSession session = new DBSession(myDataSourceName)
 *  try
 *  {
 *      session.begin();
 *      // your code to execute business logic using
 *      // more than one DAO class
 *      
 *      session.commit();
 *  }
 *  //catch exceptions if applicable
 *  finally
 *  {
 *      session.end();
 *  }
 *  
 *  - write your DAO classes extending from DAOBase as usual (SimpleTransactionalDAO also works). 
 *  Whenever they use the getConnection method they will receive the connection
 *  from the initiated database session.
 * </code>
 * 
 * For read-only APIs, the usage of this class is not required.
 * 
 * @author Suresh Kannan
 * 
 */
public final class DBSession
{
    protected static final String ERROR_MESSAGE_SESSION_NOT_INITIATED =
        "Session not initiated: ";

    protected static final String ERROR_MESSAGE_SESSION_ALREADY_INITIATED =
        "Session already initiated: ";

    private String dataSourceName = null;
    private boolean autoCommitMode;
    private Connection connection = null;

    /**
     * Creates a new session instance, but doesn't initiated it.
     * 
     * @param dataSourceName
     *            the target database name to initiate the session.
     */
    public DBSession(String dataSourceName)
    {
        this.dataSourceName = dataSourceName;
    }

    /**
     * Initiates the database session for the given database name. The
     * dataSourceName has to match the database connection pool defined on
     * <code>service_config.xml</code>, tag <code>poolUrl</code>.
     * 
     * @throws BLMException
     *             if the session for the given database name was already
     *             initiated, if the database connection can't be acquired or if
     *             a nested session is attempted.
     */
    public void begin() throws Exception
    {
        if (connection != null)
            throw new Exception(ERROR_MESSAGE_SESSION_ALREADY_INITIATED
                + dataSourceName);

        DBSessionMonitor.begin(this);

        audit("DBSession begin.");
    }

	/**
	 * Ends the database session for the given database name and returns the
	 * database connection to the connection pool.
	 * 
	 */
	public void endNoException()
	{
		try
		{
			this.end();
		}
		catch (Exception e)
		{	
				Logger logger = Logger.getLogger("DBPool");
				logger.warn(e);
		}
	}

    /**
     * Ends the database session for the given database name and returns the
     * database connection to the connection pool.
     * 
     * @throws BLMException
     *             if the session for the given database name was never
     *             initiated or the return of database connection to the pool
     *             fails.
     */
    public void end() throws Exception
    {
        if (connection == null)
            throw new Exception(ERROR_MESSAGE_SESSION_NOT_INITIATED
                + dataSourceName);

        // it is very important to remove the connection from the session before
        // invoking the Connection.close() method, because the
        // DBConnection.close will verify if it is participating on the session
        // of the current thread. If so, the connection will not be effectively
        // close and just close opened statements. If not, return the connection
        // to the pool.
        DBSessionMonitor.end(this);
        try
        {
            // any pending operation has to be undone
            connection.rollback();
            // restore the autocommit mode used before session was initiated.
            if (autoCommitMode != connection.getAutoCommit())
                connection.setAutoCommit(autoCommitMode);

            connection.close();
        }
        catch (SQLException e)
        {
            throw new Exception(e.getMessage(), e);
        }

        connection = null;
        audit("DBSession end.");
    }

    /**
     * Commits all database write operations on the give database name.
     * 
     * The connection will be available on the database session for other
     * operations, even after the commit/rollback.
     * 
     * @throws BLMException
     *             if the session for the given database name was not initiated
     *             or the database commit failed.
     */
    public void commit() throws Exception
    {
        if (connection == null)
            throw new Exception(ERROR_MESSAGE_SESSION_NOT_INITIATED
                + dataSourceName);

        try
        {
            connection.commit();
        }
        catch (SQLException e)
        {
            throw new Exception(e.getMessage(),e);
        }
        audit("DBSession commmit.");
    }

    /**
     * Rolls back all database write operations on the give database name.
     * 
     * The connection will be available on the database session for other
     * operations, even after the commit/rollback.
     * 
     * @throws BLMException
     *             if the session for the given database name was not initiated
     *             or the database commit failed.
     */
    public void rollback() throws Exception
    {
        if (connection == null)
            throw new Exception(ERROR_MESSAGE_SESSION_NOT_INITIATED
                + dataSourceName);

        try
        {
            connection.rollback();
        }
        catch (SQLException e)
        {
            throw new Exception(e.getMessage(),e);
        }
        audit("DBSession rollback.");
    }

    /**
     * Returns the database connection for the give database name if it has
     * already been initiated. Otherwise, throws an exception.
     * 
     * @return the database connection in use by this session.
     * @throws BLMException
     *             if there is no session initiated for the given database name.
     */
    public Connection getConnection()
    {
        return connection;
    }

    /**
     * Returns the database name set for this session.
     * 
     * @return the dataSourceName the database name of this session.
     */
    public String getDataSourceName()
    {
        return dataSourceName;
    }

    /**
     * Sets the database connection instance in use by this session.
     * 
     * @param conn
     *            the database connection to be used by this session.
     */
    void setConnection(Connection conn)
    {
        connection = conn;
        try
        {
            autoCommitMode = conn.getAutoCommit();
            if (autoCommitMode) conn.setAutoCommit(false);
        }
        catch (SQLException e)
        {
            // ignore this exception and just alarm it
            Logger logger =Logger.getLogger("DBPool");
            if (logger != null) logger.error(e);
        }
    }

    private void audit(String message)
    {
        Logger logger = Logger.getLogger("DBPool");
        logger.info(message + "[" + dataSourceName
                + "]. session object id:" + this);
    }
}
