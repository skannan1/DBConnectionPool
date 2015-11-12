package db.pool;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * @author Suresh Kannan
 * 
 */

class StaleConnectionManager extends Thread
{
    private DBConnectionPool pool;
    private final long delay = 180000; // check every 3 minutes
    Pattern pattern;
    StaleConnectionManager(DBConnectionPool pool)
    {
        this.pool = pool;
        setDaemon(true);
	    pattern = Pattern.compile("(Warning=)(\\d+)(%,Error=)(\\d+)(%)",Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    }

    public void run()
    {
        while (true)
        {
            try
            {
                sleep(delay);                
            }
            catch (InterruptedException e)
            {}
            
            try{
            	String alarmProperty = PoolProperties.getProperty("DataSourceAlarm");
            	if(alarmProperty!=null && alarmProperty.trim().length()>0){
	            	Matcher matcher = pattern.matcher(alarmProperty);
	            	if(matcher.find())
	            	{
	            		pool.warningLevel = Integer.parseInt(matcher.group(2));
	            		pool.errorLevel = Integer.parseInt(matcher.group(4));
	            	}
	            	else{
	            		DBConnectionPool.logger.warn("Invalid configuration for 'DataSourceAlarm'["+alarmProperty+"]" +
	            				  ".Valid configuration format is Warning=xx%,Error=xx%");
	            	}
            	}            	
            }catch(Exception ex){
				DBConnectionPool.logger.error("Error reading DataSourceAlarm",
						ex);
            }
            
            try{
            	pool.cleanStaledConnections();
            }catch(Exception ex){
				DBConnectionPool.logger.error(
						"Error cleaning staled connections", ex);
            }
            
        }
    }
}

public class DBConnectionPool
{
    private static final String ENABLE_DB_CONNECTION_TRACKER = "EnableDBConnectionTracker";
    private static final String DB_CONNECTION_WAIT_TIME = "DBConnectionWaitTime";
    private static final int DEFAULT_WAIT_TIME=5000;

    private PoolList<Connection> connections;
    private static int count = 0;
    private String url;
    private Properties props;
    private boolean autoCommit = false;
    
    final private long timeout = 300000; // check on connections that haven't
    // been used in last 5 minutes
    private StaleConnectionManager staleManager;
    private int poolsize = 10;
    private PrintStream stream = System.out;
    private int minimumSize = 0;
    private String testSQL;
    static Logger logger;
    private static boolean debugOn = false;
    int warningLevel = 60;   //default value
    int errorLevel = 80;	 //default value
    private String poolUrl;
    
    static{
        logger = Logger.getLogger("DBPool");
    }
    
    public DBConnectionPool(String url, String user, String password, boolean autoCommit,
        int minSize, int maxSize, String testSQL, String poolUrl)
    {
        this.url = url;
        this.autoCommit = autoCommit;
        
        props = new Properties();
        props.put("user", user);
        props.put("password", password);
        
        this.testSQL = testSQL;
        poolsize = maxSize;
        minimumSize = minSize;
        this.poolUrl = poolUrl;
        if(poolUrl==null)poolUrl = url;
        connections = new PoolList<Connection>();
        StringBuilder builder = new StringBuilder();
        builder.append("Creating connection pool for URL["+poolUrl+"]");
        try
        {
            for (int i = 0; i < minSize; i++)
            {
                Connection conn = createConnection();
                ((DBConnection)conn).internalClose(); // releases the lease
            }
            String _concount = minSize>1?" connections" :" connection";
            builder.append("Created "+minSize+_concount+" based on 'minSize' configuration");
            logger.info(builder.toString());
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            System.out.println("Minimum connections in pool cannot be created");
            logger.fatal("Exception occurred while initializing connection for pool "+poolUrl,ex);
        }
        staleManager = new StaleConnectionManager(this);
        staleManager.setName(poolUrl+"-Sweeper");
        staleManager.start();
    }

    @SuppressWarnings("unchecked")
    public synchronized void cleanStaledConnections()
    {
        out("Starting clean staled connections");
        long stale = System.currentTimeMillis() - timeout;
        out("Stale value is " + stale);

        // creating a copy of the list so that if we remove an entry
        // it won't screw up the iterator
        LinkedList<Connection> copy =
            (LinkedList<Connection>) connections.clone();
        Iterator<Connection> itt = copy.iterator();
        while (itt.hasNext())
        {
            DBConnection conn = (DBConnection) itt.next();
            out("Connection " + conn.getName() + " last use "
                    + conn.getLastUse());
			// a connection can't have state changed while it is being evaluated
			// to be removed from the pool, that is why it has to be synchronized here
			synchronized (conn)
			{
				if (conn.inUse()) continue;
				if (!conn.validate())
				{
					removeConnection(conn);
					continue;
				}
				if (stale > conn.getLastUse()
						&& connections.size() > minimumSize)
					removeConnection(conn);
			}
        }
        if(connections.size() < minimumSize){
        	int count = minimumSize - connections.size();
        	for(int i=0; i<count; i++){
        		Connection c;
				try {
					c = createConnection();
					((DBConnection)c).internalClose();
				} catch (SQLException e) {
					logger.warn("Unable to restore minimum number of connections in pool "+poolUrl + " due to an exception",e);
					out("Unable to restore minimum number of connections in pool "+poolUrl + " due to an exception");
					return;
				}        		
        	}
        }
        out("Pool clean up done. ["+getInUse()+"] connections are in use out of ["+connections.size()+"] in pool."+poolUrl);
    }

    public synchronized void closeConnections()
    {
        Iterator<Connection> itt = connections.iterator();
        while (itt.hasNext())
        {
            DBConnection conn = (DBConnection) itt.next();
            removeConnection(conn);
        }
    }

    private synchronized void removeConnection(DBConnection conn)
    {        
        out("removing connection ["+conn.getName()+"] from the pool");
        try{
        	conn.hardClose();
        }catch(SQLException ex){
        	out("exception while hardclosing connection:"+ex.getMessage());
        }
        connections.remove(conn);       
    }

    public int getInUse()
    {
        int inUse = 0;

        Iterator<Connection> itt = connections.iterator();
        while (itt.hasNext())
        {
            DBConnection conn = (DBConnection) itt.next();
            if (conn.inUse())
            {
                inUse++;
            }
        }
        return inUse;
    }
    
    public String getUserThreadList(){
    	StringBuilder builder = new StringBuilder();
    	Iterator<Connection> itt = connections.iterator();
        while (itt.hasNext())
        {
            DBConnection conn = (DBConnection) itt.next();
            if (conn.inUse())
            {
            	if(builder.length()>0)builder.append(",");
            	long time = System.currentTimeMillis()-conn.getCheckoutTime();
                builder.append("Checked out for["+(time)+"] "+conn.getUsingThreadName());
                builder.append(conn.getUsingStackVal());
            }
        }
        builder.append("**********************************\n");
        return builder.toString();
    }

    public int getMax()
    {
        return poolsize;
    }

    @Deprecated
    /**
     * Replaced by getMin() method. Initial size is no longer supported.
     */
    public int getInitial()
    {
        return minimumSize;
    }
    
    public int getMin()
    {
        return minimumSize;
    }
    
    public int getCurrentCount(){
    	return connections.size();
    }
    
    public int getMaxEver(){
    	return connections.getMaxEver();
    }

    public static void setDebugOn()
    {
        debugOn = true;
    }

    public static void setDebugOff()
    {
        debugOn = false;
    }

    public String getUrl()
    {
        return url;
    }

    public String getUser()
    {
        return props != null ? props.getProperty("user") : null;
    }

    private void printStat()
    {
        if (debugOn)
        {
            int inUse = getInUse();
            out("[" + inUse + "] connections are used out of "
                + connections.size() + " in pool [" + url + "].");
        }
    }

    public Connection getConnection() throws SQLException
    {
        //if there is a connection on the database session, return it!
        Connection conn = DBSessionMonitor.getSessionConnection(poolUrl);
        if (conn != null)
            return conn;

        Connection c =  getConnectionInternal();
        if(c==null) throw new SQLException("No connection available for pool "+poolUrl);
        return c;
    }
    
    
    private synchronized Connection getConnectionInternal() throws SQLException
    {
        printStat();
		try
		{
			Iterator<Connection> itt = connections.iterator();
			while (itt.hasNext())
			{
				DBConnection conn = (DBConnection) itt.next();
				// a connection can't have state changed while it is being given
				// to an executor thread, that is why it has to be synchronized
				// here
				synchronized (conn)
				{
					//also need to verify if connection has not been closed by pool 
					//during cleaning staled connection execution
					if (conn.lease() && !conn.isClosed())
					{
						out("Returning connection [" + conn.getName()
								+ "] from the pool for lease");
						conn.setUsingThreadName(Thread.currentThread().getName());
						conn.setUsingStackVal(getStackTrace());
						conn.updateCheckoutTime();
						conn.setAutoCommit(autoCommit);
						return conn;
					}
				}
			}
			if (connections.size() >= poolsize)
			{
				return getConnectionWait();
			}
			DBConnection connection = (DBConnection) createConnection();
			connection.setUsingThreadName(Thread.currentThread().getName());
			connection.setUsingStackVal(getStackTrace());
			connection.updateCheckoutTime();
			return connection;

		}
		finally
		{
        	int inUse = getInUse();
        	if((((double)inUse/poolsize)*100)>=errorLevel){
        		logger.error("[ERROR] Pool ["+poolUrl+"] reached Error alert level of "+errorLevel+"%.(" +
        				inUse+ " connections out of "+poolsize+" are in use.)");
        	}
        	else
        	if((((double)inUse/poolsize)*100)>=warningLevel){
        		logger.warn("[WARNING] Pool ["+poolUrl+"] reached Warning alert level of "+warningLevel+"%.(" +
        				inUse+ " connections out of "+poolsize+" are in use.)");
        	}
        }        
    }
    
    private String getStackTrace() {
    	String traceEnabled = PoolProperties.getProperty(ENABLE_DB_CONNECTION_TRACKER);
    	if(!"true".equals(traceEnabled)) return Thread.currentThread().getName();
		StackTraceElement[] traces = Thread.currentThread().getStackTrace();		
		StringBuilder builder = new StringBuilder();
		builder.append("\n");
		if(traces.length<2) return "";
		for(int i=2;i<traces.length;i++){
			StackTraceElement element = traces[i];
			String fileName = element.getFileName();			
			String info = null;
			if(fileName!=null) info = ":"+element.getFileName()+","+element.getLineNumber();
			builder.append(element.getClassName()+","+element.getMethodName()+(info==null?"":info));
			builder.append("\n");
		}
		return builder.toString();
	}

	private boolean isConnectionAvailable(){
    	return (getInUse()< poolsize);
    }
    
	private synchronized Connection getConnectionWait() throws SQLException
	{
		long time1= System.currentTimeMillis();
		long time2 = time1;
		
		while (!isConnectionAvailable())
		{
			int waitTime = PoolProperties.getIntProperty(DB_CONNECTION_WAIT_TIME,DEFAULT_WAIT_TIME);
			try
			{
				this.wait(waitTime - (time2-time1));								
			}
			catch (InterruptedException e){}
			finally{
				time2 = System.currentTimeMillis();
				logger.warn(Thread.currentThread().getName()+" [WARNING] Pool [" + poolUrl
						+ "] getConnection was made to wait for "+ (time2 - time1) + "ms");
			}
			Iterator<Connection> itt = connections.iterator();
			while (itt.hasNext())
			{
				DBConnection conn = (DBConnection) itt.next();
				// a connection can't have state changed while it is being given
				// to an executor thread, that is why it has to be synchronized
				// here
				synchronized (conn)
				{
					//also need to verify if connection has not been closed by pool 
					//during cleaning staled connection execution
					if (conn.lease() && !conn.isClosed())
					{
						out("Returning connection [" + conn.getName()
								+ "] from the pool for lease");
						conn.setUsingThreadName(Thread.currentThread().getName());
						conn.setUsingStackVal(getStackTrace());
						conn.updateCheckoutTime();
						conn.setAutoCommit(autoCommit);
						return conn;
					}
				}
			}
			return null;
		}
		return getConnectionInternal();
	}

    private Connection createConnection() throws SQLException
    {
        out("Creating a new connection");

        // Connection conn = DriverManager.getConnection(url, user, password);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(autoCommit);
        
        DBConnection c = new DBConnection(conn, this, testSQL);
        if (debugOn)
        {
            c.setName("Connection_" + (++count));
        }
        c.setDebug(debugOn);
        c.lease();
        connections.add(c);
        printStat();

        return c;
    }
    
    public synchronized void cleanPool(){
    	@SuppressWarnings("unchecked")
		LinkedList<Connection> copy =
                (LinkedList<Connection>) connections.clone();
            Iterator<Connection> itt = copy.iterator();
            while (itt.hasNext())
            {
                DBConnection conn = (DBConnection) itt.next();
                removeConnection(conn);
                logger.info("Connection removed from the pool by cleanPool method:" + conn.getName());
            }
            if(connections.size() < minimumSize){
            	int count = minimumSize - connections.size();
            	for(int i=0; i<count; i++){
            		Connection c;
    				try {
    					c = createConnection();
    					((DBConnection)c).internalClose();
    				} catch (SQLException e) {
    					logger.warn("Unable to restore minimum number of connections in pool "+poolUrl + " due to an exception",e);
    					out("Unable to restore minimum number of connections in pool "+poolUrl + " due to an exception");
    					return;
    				}        		
            	}
            }
            notifyAll();
    }

    public synchronized void returnConnection(DBConnection conn)
    {
        try
        {
            if (conn.isClosed())
            {
                removeConnection(conn);               
                return;
            }              
            conn.expireLease();
            conn.setUsingThreadName("");
            conn.setUsingStackVal("");
            conn.clearCheckoutTime();
            printStat();
        }catch (SQLException e){}
        finally{
        	notify();
        }
    }

    /**
     * @return Returns the debugOn.
     */
    public static boolean isDebugOn()
    {
        return debugOn;
    }

    /**
     * @param debugOn
     *            The debugOn to set.
     */
    public static void setDebugOn(boolean debugOn)
    {
        DBConnectionPool.debugOn = debugOn;
    }

    /**
     * @param stream
     *            The stream to set.
     */
    public void setStream(PrintStream stream)
    {
        this.stream = stream;
    }

    private void out(String output)
    {
        if (debugOn) stream.println(output);
    }

    /**
     * @return the poolUrl
     */
    protected String getPoolUrl()
    {
        return poolUrl;
    }
}

class PoolList<E> extends LinkedList<E>{
	
	private static final long serialVersionUID = 1L;
	private int maxEver=0;

	@Override
	public boolean add(E e) {		
		boolean b = super.add(e);
		if(size()>maxEver) maxEver = size();
		return b;
	}

	@Override
	public boolean remove(Object o) {
		return super.remove(o);
	}
	
	public int getMaxEver(){
		return maxEver;
	}
	
}
