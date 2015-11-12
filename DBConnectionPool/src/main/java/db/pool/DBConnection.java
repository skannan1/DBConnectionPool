package db.pool;

import java.io.PrintStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import org.apache.log4j.Logger;

/**
 * @author Suresh Kannan
 * 
 *         
 */

public class DBConnection implements Connection
{

    private static final String DB_QUERY_TIMEOUT = "DbQueryTimeout";
    private long checkouttime = 0;
    
    private DBConnectionPool pool;
    // stores all statements created by this connection in order to close them
    // when the connection is "closed" (released to the pool)
    private List<Statement> statements = new ArrayList<Statement>(10);
    /**
     * @param stream
     *            The stream to set.
     */

    private Connection conn;
    private boolean inuse;
    private long timestamp;
    private String name;
    private boolean debugOn = false;
    private PrintStream stream = System.out;
    private PreparedStatement testQuery;
    private String testSQL; 
    private String usingThreadName="";
    private String usingStackVal = "";

    private Logger logger;
    private boolean auditEnabled = false;

    public DBConnection(Connection conn, DBConnectionPool pool, String testSQL)
    {
        this.conn = conn;
        this.pool = pool;
        this.inuse = false;
        this.timestamp = 0;
        this.testSQL = testSQL;
        setTestQuery(testSQL);
        statements = new ArrayList<Statement>();
    }

	protected boolean lease()
	{
		if (inuse)
		{
			return false;
		}
		else
		{
			inuse = true;
			timestamp = System.currentTimeMillis();
			return true;
		}
	}

    private void out(String output)
    {
        stream.println("[DBConnection:" + getName() + "]" + output);
    }

    public boolean validate()
    {
        try
        {
            if (testSQL == null || testSQL.trim().length() == 0)
            {
                out("TestSQL for connection validity test is empty. "
                    + "Connection test is being skipped");
                return true;
            }
            if (testQuery == null) setTestQuery(testSQL);
            if (testQuery == null)
            {
                if (debugOn) out("Connection validation failed");
                return false;
            }
            testQuery.execute();
            if (debugOn) out("Connection validation passed");
        }
        catch (Exception e)
        {
            if (debugOn) out("Connection validation failed");
            return false;
        }
        return true;
    }

	protected boolean inUse()
	{
		return inuse;
	}

    public long getLastUse()
    {
        return timestamp;
    }

    void setName(String name)
    {
        this.name = name;
    }

    String getName()
    {
        return name;
    }

    public void setDebug(boolean flag)
    {
        debugOn = flag;
    }

    public boolean isDebugOn()
    {
        return debugOn;
    }

    public void close() throws SQLException
    {
    	if(!Thread.currentThread().getName().equals(usingThreadName)){
    		return;
    	}
        logger = null;
        auditEnabled = false;

        closeStatements();
        
        //Only effectively close the connection and return it to the pool
        //if it is not part of an DBSession
        if (!DBSessionMonitor.contains(this))
        {
            if (conn.getAutoCommit())
                conn.commit();
            else conn.rollback();

            if (debugOn) out("Returning connection to the pool");
            pool.returnConnection(this);
        }
    }
    
    void internalClose() throws SQLException
    {
        logger = null;
        auditEnabled = false;

        closeStatements();
        
        //Only effectively close the connection and return it to the pool
        //if it is not part of an DBSession
        if (!DBSessionMonitor.contains(this))
        {
            if (conn.getAutoCommit())
                conn.commit();
            else conn.rollback();

            if (debugOn) out("Returning connection to the pool");
            pool.returnConnection(this);
        }
    }

    protected void expireLease()
    {
        inuse = false;
        if (debugOn) out("Expired lease");
    }

    protected Connection getConnection()
    {
        return conn;
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException
    {
        DBPreparedStatement impl =
            new DBPreparedStatement(conn.prepareStatement(sql), sql);
        impl.setQueryTimeout(getQueryTimeout());
        statements.add(impl);
        return (PreparedStatement) LogHandler1.newInstance(impl, auditEnabled
            ? logger : null, this.getPool().getPoolUrl());
    }

    public CallableStatement prepareCall(String sql) throws SQLException
    {
        DBCallableStatement impl =
            new DBCallableStatement(conn.prepareCall(sql),sql);
        impl.setQueryTimeout(getQueryTimeout());
        statements.add(impl);
        return (CallableStatement) LogHandler1.newInstance(impl, auditEnabled
                ? logger : null, this.getPool().getPoolUrl());       
    }

    public Statement createStatement() throws SQLException
    {
        DBStatement impl = new DBStatement(conn.createStatement());
        statements.add(impl);
        return (Statement) LogHandler1.newInstance(impl, auditEnabled
                ? logger : null, this.getPool().getPoolUrl());
    }

    public String nativeSQL(String sql) throws SQLException
    {
        return conn.nativeSQL(sql);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        //Only effective if the connection is not part of an DBSession
        if (!DBSessionMonitor.contains(this))
            conn.setAutoCommit(autoCommit);
    }

    public boolean getAutoCommit() throws SQLException
    {
        return conn.getAutoCommit();
    }

    public void commit() throws SQLException
    {     
        conn.commit();
    }

    public void rollback() throws SQLException
    {
        conn.rollback();        
    }

    public boolean isClosed() throws SQLException
    {
        return conn.isClosed();
    }

    public DatabaseMetaData getMetaData() throws SQLException
    {
        return conn.getMetaData();
    }

    public void setReadOnly(boolean readOnly) throws SQLException
    {
        conn.setReadOnly(readOnly);
    }

    public boolean isReadOnly() throws SQLException
    {
        return conn.isReadOnly();
    }

    public void setCatalog(String catalog) throws SQLException
    {
        conn.setCatalog(catalog);
    }

    public String getCatalog() throws SQLException
    {
        return conn.getCatalog();
    }

    public void setTransactionIsolation(int level) throws SQLException
    {
        conn.setTransactionIsolation(level);
    }

    public int getTransactionIsolation() throws SQLException
    {
        return conn.getTransactionIsolation();
    }

    public SQLWarning getWarnings() throws SQLException
    {
        return conn.getWarnings();
    }

    public void clearWarnings() throws SQLException
    {
        conn.clearWarnings();
    }

    /**
     * @param resultSetType
     * @param resultSetConcurrency
     * @return
     * @throws java.sql.SQLException
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
        throws SQLException
    {
        DBStatement impl =
            new DBStatement(conn.createStatement(resultSetType, resultSetConcurrency));
        statements.add(impl);
        return LogHandler1.newInstance(impl, auditEnabled
                ? logger : null, this.getPool().getPoolUrl());
    }

    /**
     * @param resultSetType
     * @param resultSetConcurrency
     * @param resultSetHoldability
     * @return
     * @throws java.sql.SQLException
     */
    public Statement createStatement(int resultSetType,
        int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        DBStatement impl =
            new DBStatement(conn.createStatement(resultSetType, resultSetConcurrency,
                resultSetHoldability));
        statements.add(impl);
        return LogHandler1.newInstance(impl, auditEnabled
                ? logger : null, this.getPool().getPoolUrl());
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj)
    {
        if (obj instanceof DBConnection)
        {
            return super.equals(obj);
        }
        else
        {
            return conn.equals(obj);
        }
    }

    /**
     * @return
     * @throws java.sql.SQLException
     */
    public int getHoldability() throws SQLException
    {
        return conn.getHoldability();
    }

    /**
     * @return
     * @throws java.sql.SQLException
     */
    public Map<String, Class<?>> getTypeMap() throws SQLException
    {
        return conn.getTypeMap();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    public int hashCode()
    {
        return conn.hashCode();
    }

    /**
     * @param sql
     * @param resultSetType
     * @param resultSetConcurrency
     * @return
     * @throws java.sql.SQLException
     */
    public CallableStatement prepareCall(String sql, int resultSetType,
        int resultSetConcurrency) throws SQLException
    {
        DBCallableStatement impl =
            new DBCallableStatement(conn.prepareCall(sql, resultSetType,
                resultSetConcurrency),sql);
        impl.setQueryTimeout(getQueryTimeout());
        statements.add(impl);
        return impl;
    }

    /**
     * @param sql
     * @param resultSetType
     * @param resultSetConcurrency
     * @param resultSetHoldability
     * @return
     * @throws java.sql.SQLException
     */
    public CallableStatement prepareCall(String sql, int resultSetType,
        int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        DBCallableStatement impl =
            new DBCallableStatement(conn.prepareCall(sql, resultSetType,
                resultSetConcurrency, resultSetHoldability),sql);
        impl.setQueryTimeout(getQueryTimeout());
        statements.add(impl);
        return impl;
    }

    /**
     * @param sql
     * @param autoGeneratedKeys
     * @return
     * @throws java.sql.SQLException
     */
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
        throws SQLException
    {
        DBPreparedStatement impl =
            new DBPreparedStatement(conn.prepareStatement(sql,
                autoGeneratedKeys), sql);
        impl.setQueryTimeout(getQueryTimeout());
        statements.add(impl);
        return (PreparedStatement) LogHandler1.newInstance(impl, auditEnabled
            ? logger : null, this.getPool().getPoolUrl());
    }

    /**
     * @param sql
     * @param resultSetType
     * @param resultSetConcurrency
     * @return
     * @throws java.sql.SQLException
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType,
        int resultSetConcurrency) throws SQLException
    {
        DBPreparedStatement impl =
            new DBPreparedStatement(conn.prepareStatement(sql, resultSetType,
                resultSetConcurrency), sql);
        impl.setQueryTimeout(getQueryTimeout());
        statements.add(impl);
        return (PreparedStatement) LogHandler1.newInstance(impl, auditEnabled
            ? logger : null, this.getPool().getPoolUrl());
    }

	private int getQueryTimeout() {
		int timeout = PoolProperties.getIntProperty(DB_QUERY_TIMEOUT,30);
		return timeout;
	}

    /**
     * @param sql
     * @param resultSetType
     * @param resultSetConcurrency
     * @param resultSetHoldability
     * @return
     * @throws java.sql.SQLException
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType,
        int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        DBPreparedStatement impl =
            new DBPreparedStatement(conn.prepareStatement(sql, resultSetType,
                resultSetConcurrency, resultSetHoldability), sql);
        impl.setQueryTimeout(getQueryTimeout());
        statements.add(impl);
        return (PreparedStatement) LogHandler1.newInstance(impl, auditEnabled
            ? logger : null, this.getPool().getPoolUrl());
    }

    /**
     * @param sql
     * @param columnIndexes
     * @return
     * @throws java.sql.SQLException
     */
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
        throws SQLException
    {
        DBPreparedStatement impl =
            new DBPreparedStatement(conn.prepareStatement(sql, columnIndexes),
                sql);
        impl.setQueryTimeout(getQueryTimeout());
        statements.add(impl);
        return (PreparedStatement) LogHandler1.newInstance(impl, auditEnabled
            ? logger : null, this.getPool().getPoolUrl());
    }

    /**
     * @param sql
     * @param columnNames
     * @return
     * @throws java.sql.SQLException
     */
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
        throws SQLException
    {
        DBPreparedStatement impl =
            new DBPreparedStatement(conn.prepareStatement(sql, columnNames),
                sql);
        impl.setQueryTimeout(getQueryTimeout());
        statements.add(impl);
        return (PreparedStatement) LogHandler1.newInstance(impl, auditEnabled
            ? logger : null, this.getPool().getPoolUrl());
    }

    /**
     * @param savepoint
     * @throws java.sql.SQLException
     */
    public void releaseSavepoint(Savepoint savepoint) throws SQLException
    {
        conn.releaseSavepoint(savepoint);
    }

    /**
     * @param savepoint
     * @throws java.sql.SQLException
     */
    public void rollback(Savepoint savepoint) throws SQLException
    {
        conn.rollback(savepoint);
    }

    /**
     * @param holdability
     * @throws java.sql.SQLException
     */
    public void setHoldability(int holdability) throws SQLException
    {
        conn.setHoldability(holdability);
    }

    /**
     * @return
     * @throws java.sql.SQLException
     */
    public Savepoint setSavepoint() throws SQLException
    {
        return conn.setSavepoint();
    }

    /**
     * @param name
     * @return
     * @throws java.sql.SQLException
     */
    public Savepoint setSavepoint(String name) throws SQLException
    {
        return conn.setSavepoint(name);
    }

    /**
     * @param map
     * @throws java.sql.SQLException
     */
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException
    {
        conn.setTypeMap(map);
    }

    public void setStream(PrintStream stream)
    {
        this.stream = stream;
    }

    public void setTestQuery(String testSQL)
    {
        this.testSQL = testSQL;
        try
        {
            if (testQuery != null) testQuery.close();
        }
        catch (SQLException ex)
        {}
        try
        {
            testQuery = conn.prepareStatement(testSQL);
        }
        catch (SQLException ex1)
        {}
    }

    void hardClose() throws SQLException
    {
        closeStatements();
        if (conn != null) conn.close();
    }

    public void setLogger(Logger logger)
    {
        this.logger = logger;
    }

    protected void closeStatements()
    {
        for (Iterator<Statement> iter = statements.iterator(); iter.hasNext();)
        {
            Statement element = iter.next();
            try
            {
                element.close();
            }
            catch (SQLException e)
            {
                if (logger != null && logger.isTraceEnabled()) logger.trace(e);
            }
        }
        statements.clear();
    }

    /**
     * If <code>true</code> allows audit log accoding to the <code>Logger</code>
     * setting. Otherwise, audit log of the SQL will be disabled regardless the
     * setting of <code>Logger</code>. By default, it is disabled.
     * 
     * @param b
     *            if <code>true</code>, allow SQL audit log according to
     *            <code>Logger</code> setting. Otherwise, the SQL won't be
     *            available on audit log.
     */
    public void enableAudit(boolean b)
    {
        auditEnabled = b;
    }

	public <T> T unwrap(Class<T> iface) throws SQLException {
		return conn.unwrap(iface);
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
	   return conn.isWrapperFor(iface);
	}

	public Clob createClob() throws SQLException {
		return conn.createClob();
	}

	public Blob createBlob() throws SQLException {
		return conn.createBlob();
	}

	public NClob createNClob() throws SQLException {
		return conn.createNClob();
	}

	public SQLXML createSQLXML() throws SQLException {
		return conn.createSQLXML();
	}

	public boolean isValid(int timeout) throws SQLException {
		return conn.isValid(timeout);
	}

	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		conn.setClientInfo(name, value);
	}

	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		conn.setClientInfo(properties);
	}

	public String getClientInfo(String name) throws SQLException {
		return conn.getClientInfo(name);
	}

	public Properties getClientInfo() throws SQLException {
		return conn.getClientInfo();
	}

	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		return conn.createArrayOf(typeName, elements);
	}

	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		return conn.createStruct(typeName, attributes);
	}

	public String getUsingThreadName() {
		return usingThreadName;
	}

	public void setUsingThreadName(String threadName) {
		this.usingThreadName = threadName;
	}
	
	public String getUsingStackVal() {
		return usingStackVal;
	}

	public void setUsingStackVal(String stackVal) {
		this.usingStackVal = stackVal;
	}
	
	public void updateCheckoutTime(){
		checkouttime = System.currentTimeMillis();
	}
	
	public long getCheckoutTime(){
		return checkouttime;
	}
	
	public void clearCheckoutTime(){
		checkouttime = -1;
	}

    /**
     * @return the pool
     */
    protected DBConnectionPool getPool()
    {
        return pool;
    }

	public void setSchema(String schema) throws SQLException {	
	}

	public String getSchema() throws SQLException {
		return null;
	}

	public void abort(Executor executor) throws SQLException {
	}

	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
	}

	public int getNetworkTimeout() throws SQLException {
		return 0;
	}    
   
}

class LogHandler1 implements InvocationHandler
{
    private static final String PROPERTY_NAME_MAXTIME = "DBStatementWarningThreshold";
    private static final int PROPERTY_DEFAULT_VALUE_MAXTIME = 10000;
    private static final String ALARM_MESSAGE_MAXTIME1 =
        "[WARNING] SQL statement execution exceeded the threshold of ";
    private static final String ALARM_MESSAGE_MAXTIME2 =
        "(ms), defined by property name " + PROPERTY_NAME_MAXTIME + "\n";
    private static final String LOG_MESSAGE_ELAPSEDTIME =
        "\nElapsed time (ms) =";
    private static final String METHOD_NAME_PREFIX = "execute";
    
    private Logger logger;
    private DBStatement ps;
    private String dataSourceUrl;

    private LogHandler1(DBStatement ps, Logger logger, String dataSourceName)
    {
        this.ps = ps;
        this.logger = logger;
        this.dataSourceUrl = dataSourceName;
    }
    
    public static Statement newInstance(DBStatement ps,
    		Logger logger, String dataSourceName)
    {
    	return (Statement) java.lang.reflect.Proxy.newProxyInstance(ps
    			.getClass().getClassLoader(), ps.getClass().getInterfaces(),
    			new LogHandler1(ps, logger, dataSourceName));
    }

    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable
    {
        String meth = method.getName();
        long starttime = System.currentTimeMillis();
        Object obj;
        try
        {
            obj = method.invoke(ps, args);
        }
        catch (InvocationTargetException e)
        {
            // throws the original exception raised by the invoked method and
            // wrapped by this exception, if available.
            Throwable t = e.getCause();
            throw (t != null ? t : e);
        }
        finally
        {
            long elapsedtime = System.currentTimeMillis() - starttime;
            if (logger != null && meth.startsWith(METHOD_NAME_PREFIX))
            {
                String logmessage =
                		Thread.currentThread().getName()+ps.toString() +"  "
                    + "\n" + this.dataSourceUrl 
                    + LOG_MESSAGE_ELAPSEDTIME + elapsedtime;
                long maxtime =
                    PoolProperties.getIntProperty(PROPERTY_NAME_MAXTIME, PROPERTY_DEFAULT_VALUE_MAXTIME);

                if (elapsedtime > maxtime)
                    logger.warn(ALARM_MESSAGE_MAXTIME1 + maxtime
                        + ALARM_MESSAGE_MAXTIME2 + logmessage);
                logger.info(logmessage);
            }
        }
        return obj;
    }    
   
}
