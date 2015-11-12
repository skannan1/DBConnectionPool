package db.pool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;


/**
 * Factory class to retrieve database connection from application connection
 * pool.
 * 
 * @author Suresh Kannan
 * 
 */
public class DBConnectionFactory
{

	/**
	 * Retrieves a connection from the specified datasource connection pool
	 * name. The connection instance will audit all executed statements if the
	 * audit log is set and turn on.
	 * 
	 * @param dataSourceName
	 *            the connection pool datasource name
	 * @return the connection instance from the pool.
	 * @throws DatabaseException
	 *             if any error occurs or there is no connection available on
	 *             the pool.
	 */
	public static Connection getConnection(String dataSourceName)
			throws SQLException
	{
		return getConnection(dataSourceName, true);
	}

	/**
	 * Retrieves a connection from the specified datasource connection pool
	 * name. The connection instance will audit all executed statements if the
	 * audit log is set and turn on.
	 * 
	 * Use this method if it is required to be able to turn off on audit log the
	 * sql statement only. The <code>auditEnabled</code> parameter is supposed
	 * to be read by the called from the configuration file of the application.
	 * 
	 * @param dataSourceName
	 *            the connection pool datasource name
	 * @param auditEnabled
	 *            determinates whether to audit sql statement executed by the
	 *            connection instance returned or not. <code>true</code> audit
	 *            sql statement if audit log is enabled and turned on,
	 *            <code>false</code> never logs the sql statements executed.
	 * @return the connection instance from the pool.
	 * @throws DatabaseException
	 *             if any error occurs or there is no connection available on
	 *             the pool.
	 */
	public static Connection getConnection(String dataSourceName,
			boolean auditEnabled) throws SQLException
	{
		Connection conn;
		conn = DriverManager.getConnection(dataSourceName);
		if (conn instanceof DBConnection){
				Logger logger = Logger.getLogger("DBPoolFactory");
				DBConnection dbConn = ((DBConnection) conn);
				dbConn.enableAudit(auditEnabled && logger != null
						&& "true".equals(PoolProperties.getProperty("EnableAudit")));
		}
		return conn;
	}
}
