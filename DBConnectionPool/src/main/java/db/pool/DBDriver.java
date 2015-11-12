package db.pool;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @author Suresh Kannan
 * 
 */

public class DBDriver implements Driver
{

    public String URL = "jdbc:db:";
    private static final int MAJOR_VERSION = 1;
    private static final int MINOR_VERSION = 0;
    private DBConnectionPool pool;

    /**
     * @param debugOn
     *            The debugOn to set.
     */
    public static void setDebugOn(boolean debugOn)
    {
        DBConnectionPool.setDebugOn(debugOn);
    }

    public DBDriver(String driver, String url, String user, String password,
        int maxSize, int minSize, String poolUrl, String testSQL)
        throws ClassNotFoundException, InstantiationException,
        IllegalAccessException, SQLException
    {
        URL = poolUrl;
        if (minSize > maxSize)
        {
            throw new IllegalAccessException(
                "Minimum pool size cannot be higher than Maximum pool size");
        }
        DriverManager.registerDriver(this);
        Class.forName(driver);
        pool =
            new DBConnectionPool(url, user, password, false, minSize, maxSize,
                testSQL,poolUrl);
    }

    public DBDriver(String driver, String url, String user, String password,
        boolean autoCommit, int maxSize, int minSize, String poolUrl,
        String testSQL) throws ClassNotFoundException, InstantiationException,
        IllegalAccessException, SQLException
    {
        URL = poolUrl;
        if (minSize > maxSize)
        {
            throw new IllegalAccessException(
                "Initial pool size cannot be higher than pool size");
        }
        DriverManager.registerDriver(this);
        Class.forName(driver);
        pool =
            new DBConnectionPool(url, user, password, autoCommit, minSize,
                maxSize, testSQL,poolUrl);
    }

    public Connection connect(String url, Properties props) throws SQLException
    {
        if (!url.equalsIgnoreCase(URL)) return null;

        if (pool == null)
            throw new NullPointerException(
                "Pool not initialized. Check Server alarm log.");

        return pool.getConnection();
    }

    public boolean acceptsURL(String url)
    {
        return url.equalsIgnoreCase(URL);
    }

    public int getMajorVersion()
    {
        return MAJOR_VERSION;
    }

    public int getMinorVersion()
    {
        return MINOR_VERSION;
    }

    public DriverPropertyInfo[] getPropertyInfo(String str, Properties props)
    {
        return new DriverPropertyInfo[0];
    }

    public String getURL()
    {
        return URL;
    }

    public boolean jdbcCompliant()
    {
        return false;
    }

    public DBConnectionPool getPool()
    {
        return pool;
    }

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}
}
