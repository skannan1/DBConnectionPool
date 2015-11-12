/**
 * 
 */
package db.pool;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;


/**
 * This class monitors the DBSession instances.
 * 
 * It can detect database connection leak, caused by missing
 * <code>DBSession.end()</code> for initiated database sessions.
 * 
 * This class is supposed to be used by the framework only.
 * 
 * @author lgobi
 * 
 */
public final class DBSessionMonitor
{
    protected static final String ERROR_MESSAGE_NESTED_SESSION =
        "Nested session is not allow. End current session before begining a new session. Datasource: ";
    protected static final String ERROR_MESSAGE_NULL_DATASOURCE =
        "Unable to begin database session for null datasource name.";
    protected static final String ERROR_MESSAGE_CONNECTION_LEAK =
        "Database connection leak detected for datasource names: ";
    protected static final String ERROR_MESSAGE_SESSION_MISSING =
        "Session not initiated. Non existent session found for datasource: ";

    private static ThreadLocal<Map<String, DBSession>> localSessionMap =
        new ThreadLocal<Map<String, DBSession>>();

    // utility class should not be instantiated.
    private DBSessionMonitor()
    {}

    /**
     * Checks whether or not there is open (initiated but not ended) database
     * session, which would mean connection leak. If so, close them and throw
     * exception.
     * 
     * @throws DatabaseSystemException
     *             if there is connection leak detected.
     */
    public static void checkConnectionLeak() throws Exception
    {
        List<DBSession> sessionList = getOpenedSessions();
        if (sessionList != null && !sessionList.isEmpty())
        {
            Logger logger = Logger.getLogger("DBPool");
            // force session end and rollback
            List<String> dataSourceNameList =
                new ArrayList<String>(sessionList.size());
            for (DBSession session : sessionList)
            {
                dataSourceNameList.add(session.getDataSourceName());
                try
                {
                    session.end();
                    if (logger != null)
                        logger
                            .error("Connection leak detected for datasource: "
                                + session.getDataSourceName()
                                + ". Connection was rollback and returned to the pool.");
                }
                catch (Exception e)
                {
                    if (logger != null) logger.error(e);
                }
            }

            throw new Exception(ERROR_MESSAGE_CONNECTION_LEAK
                + dataSourceNameList.toString());
        }
    }

    /**
     * Returns the list of session objects currently opened (initiated but not
     * ended) on this database session.
     * 
     * @return the list of opened sessions, or <code>null</code> if none
     *         database session has ever been initiated. The list will be empty
     *         if all initiated sessions have already been ended.
     */
    public static List<DBSession> getOpenedSessions()
    {
        Map<String, DBSession> sessionMap = localSessionMap.get();
        if (sessionMap != null)
        {
            List<DBSession> list = new ArrayList<DBSession>();
            Set<Entry<String, DBSession>> entries = sessionMap.entrySet();
            for (Entry<String, DBSession> entry : entries)
            {
                list.add(entry.getValue());
            }
            return list;
        }
        else return null;
    }

    /**
     * Returns the database connection for the given database name that is being
     * held on the database session, or <code>null</code> if none.
     * 
     * @param dataSourceName
     *            the database name
     * @return the current connection on the database session for the given
     *         database name. Or <code>null</code> if there is none.
     */
    static Connection getSessionConnection(String dataSourceName)
    {
        Map<String, DBSession> sessionMap = localSessionMap.get();
        DBSession session =
            (sessionMap != null && sessionMap.containsKey(dataSourceName))
                ? sessionMap.get(dataSourceName) : null;

        return session == null ? null : session.getConnection();
    }

    /**
     * Returns <code>true</code> if the database connection is on the current
     * session. Otherwise <code>false</code>.
     * 
     * This method checks if the data source name of the given database
     * connection has a database session initiated.
     * 
     * @param conn
     *            the database connection to be verified.
     * @return <code>true</code> if the database connection is on the current
     *         session. Otherwise <code>false</code>.
     */
    static boolean contains(DBConnection conn)
    {
        if (conn == null) return false;

        Map<String, DBSession> sessionMap = localSessionMap.get();
        if (sessionMap == null) return false;

        DBConnectionPool pool = conn.getPool();
        return pool == null ? false : sessionMap.containsKey(pool.getPoolUrl());
    }

    /**
     * Initiates the database session for the given database name. The
     * dataSourceName has to match the database connection pool defined on
     * <code>service_config.xml</code>, tag <code>poolUrl</code>.
     * 
     * @param dataSourceName
     *            the name of the database connection pool
     * @throws BLMException
     *             if the session for the given database name was already
     *             initiated or the database connection can't be acquired.
     */
    static void begin(DBSession session) throws Exception
    {
        if (session == null) return;

        String dataSourceName = session.getDataSourceName();
        if (dataSourceName == null)
            throw new Exception(ERROR_MESSAGE_NULL_DATASOURCE);

        Map<String, DBSession> sessionMap = localSessionMap.get();
        if (sessionMap == null)
        {
            sessionMap = new HashMap<String, DBSession>();
            localSessionMap.set(sessionMap);
        }
        else if (sessionMap.containsKey(dataSourceName))
            throw new Exception(ERROR_MESSAGE_NESTED_SESSION
                + dataSourceName);

        session
            .setConnection(DBConnectionFactory.getConnection(dataSourceName));

        // now the connection is tied to the session and its autocommit mode
        // can't be change externally
        sessionMap.put(dataSourceName, session);
    }

    /**
     * Ends the database session for the given database name and returns the
     * database connection to the connection pool.
     * 
     * @param dataSourceName
     *            the name of the database connection pool
     * @throws BLMException
     *             if the session for the given database name was never
     *             initiated or the return of database connection to the pool
     *             fails.
     */
    static void end(DBSession session) throws Exception
    {
        if (session == null) return;

        Map<String, DBSession> sessionMap = localSessionMap.get();
        if (sessionMap == null
            || sessionMap.remove(session.getDataSourceName()) == null)
            throw new Exception(ERROR_MESSAGE_SESSION_MISSING
                + session.getDataSourceName());
    }

}
