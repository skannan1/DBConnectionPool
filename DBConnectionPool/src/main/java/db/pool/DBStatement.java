/*
 * Created on Aug 10, 2009
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package db.pool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Suresh Kannan
 * 
 *         This is a wrapper class created to control the result set close when
 *         this statement is closed.
 */
public class DBStatement implements Statement
{

    protected Statement wrappedStatement;
    private List<ResultSet> resultSetList = new ArrayList<ResultSet>(10);
	private String sql;
	private int[] columnIndexes;
	private String[] columnNames;
	private static final String simpleName = DBStatement.class.getSimpleName();

    protected DBStatement(Statement statement)
    {
        wrappedStatement = statement;
    }

    protected ResultSet add(ResultSet rs)
    {
        resultSetList.add(rs);
        return rs;
    }

    protected void closeResultSets()
    {
        for (Iterator<ResultSet> iter = resultSetList.iterator(); iter
            .hasNext();)
        {
            ResultSet element = iter.next();
            try
            {
                element.close();
            }
            catch (SQLException e)
            {
                // just ignored
            }
        }
        resultSetList.clear();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeQuery(java.lang.String)
     */
    public ResultSet executeQuery(String sql) throws SQLException
    {
    	this.sql = sql;
        return add(wrappedStatement.executeQuery(sql));
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeUpdate(java.lang.String)
     */
    public int executeUpdate(String sql) throws SQLException
    {
    	this.sql = sql;
        return wrappedStatement.executeUpdate(sql);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#close()
     */
    public void close() throws SQLException
    {
        closeResultSets();
        wrappedStatement.close();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getMaxFieldSize()
     */
    public int getMaxFieldSize() throws SQLException
    {
        return wrappedStatement.getMaxFieldSize();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setMaxFieldSize(int)
     */
    public void setMaxFieldSize(int max) throws SQLException
    {
        wrappedStatement.setMaxFieldSize(max);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getMaxRows()
     */
    public int getMaxRows() throws SQLException
    {
        return wrappedStatement.getMaxRows();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setMaxRows(int)
     */
    public void setMaxRows(int max) throws SQLException
    {
        wrappedStatement.setMaxRows(max);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setEscapeProcessing(boolean)
     */
    public void setEscapeProcessing(boolean enable) throws SQLException
    {
        wrappedStatement.setEscapeProcessing(enable);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getQueryTimeout()
     */
    public int getQueryTimeout() throws SQLException
    {
        return wrappedStatement.getQueryTimeout();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setQueryTimeout(int)
     */
    public void setQueryTimeout(int seconds) throws SQLException
    {
        wrappedStatement.setQueryTimeout(seconds);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#cancel()
     */
    public void cancel() throws SQLException
    {
        closeResultSets();
        wrappedStatement.cancel();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getWarnings()
     */
    public SQLWarning getWarnings() throws SQLException
    {
        return wrappedStatement.getWarnings();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#clearWarnings()
     */
    public void clearWarnings() throws SQLException
    {
        wrappedStatement.clearWarnings();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setCursorName(java.lang.String)
     */
    public void setCursorName(String name) throws SQLException
    {
        wrappedStatement.setCursorName(name);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#execute(java.lang.String)
     */
    public boolean execute(String sql) throws SQLException
    {
    	this.sql = sql;
        return wrappedStatement.execute(sql);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getResultSet()
     */
    public ResultSet getResultSet() throws SQLException
    {
        return add(wrappedStatement.getResultSet());
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getUpdateCount()
     */
    public int getUpdateCount() throws SQLException
    {
        return wrappedStatement.getUpdateCount();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getMoreResults()
     */
    public boolean getMoreResults() throws SQLException
    {
        return wrappedStatement.getMoreResults();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setFetchDirection(int)
     */
    public void setFetchDirection(int direction) throws SQLException
    {
        wrappedStatement.setFetchDirection(direction);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getFetchDirection()
     */
    public int getFetchDirection() throws SQLException
    {
        return wrappedStatement.getFetchDirection();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#setFetchSize(int)
     */
    public void setFetchSize(int rows) throws SQLException
    {
        wrappedStatement.setFetchSize(rows);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getFetchSize()
     */
    public int getFetchSize() throws SQLException
    {
        return wrappedStatement.getFetchSize();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getResultSetConcurrency()
     */
    public int getResultSetConcurrency() throws SQLException
    {
        return wrappedStatement.getResultSetConcurrency();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getResultSetType()
     */
    public int getResultSetType() throws SQLException
    {
        return wrappedStatement.getResultSetType();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#addBatch(java.lang.String)
     */
    public void addBatch(String sql) throws SQLException
    {
        wrappedStatement.addBatch(sql);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#clearBatch()
     */
    public void clearBatch() throws SQLException
    {
        wrappedStatement.clearBatch();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeBatch()
     */
    public int[] executeBatch() throws SQLException
    {
        return wrappedStatement.executeBatch();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getConnection()
     */
    public Connection getConnection() throws SQLException
    {
        return wrappedStatement.getConnection();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getMoreResults(int)
     */
    public boolean getMoreResults(int current) throws SQLException
    {
        return wrappedStatement.getMoreResults(current);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getGeneratedKeys()
     */
    public ResultSet getGeneratedKeys() throws SQLException
    {
        return add(wrappedStatement.getGeneratedKeys());
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeUpdate(java.lang.String, int)
     */
    public int executeUpdate(String sql, int autoGeneratedKeys)
        throws SQLException
    {
    	this.sql = sql;
        return wrappedStatement.executeUpdate(sql, autoGeneratedKeys);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
     */
    public int executeUpdate(String sql, int[] columnIndexes)
        throws SQLException
    {
    	this.sql = sql;
    	this.columnIndexes = columnIndexes;
        return wrappedStatement.executeUpdate(sql, columnIndexes);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#executeUpdate(java.lang.String,
     * java.lang.String[])
     */
    public int executeUpdate(String sql, String[] columnNames)
        throws SQLException
    {
    	this.sql = sql;
    	this.columnNames = columnNames;
        return wrappedStatement.executeUpdate(sql, columnNames);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#execute(java.lang.String, int)
     */
    public boolean execute(String sql, int autoGeneratedKeys)
        throws SQLException
    {
    	this.sql = sql;
    	return wrappedStatement.execute(sql, autoGeneratedKeys);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#execute(java.lang.String, int[])
     */
    public boolean execute(String sql, int[] columnIndexes) throws SQLException
    {
    	this.sql = sql;
    	this.columnIndexes = columnIndexes;
        return wrappedStatement.execute(sql, columnIndexes);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
     */
    public boolean execute(String sql, String[] columnNames)
        throws SQLException
    {
    	this.sql = sql;
    	this.columnNames = columnNames;
    	return wrappedStatement.execute(sql, columnNames);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Statement#getResultSetHoldability()
     */
    public int getResultSetHoldability() throws SQLException
    {
        return wrappedStatement.getResultSetHoldability();
    }

	public <T> T unwrap(Class<T> iface) throws SQLException {
		return wrappedStatement.unwrap(iface);
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return wrappedStatement.isWrapperFor(iface);
	}

	public boolean isClosed() throws SQLException {
		return wrappedStatement.isClosed();
	}

	public void setPoolable(boolean poolable) throws SQLException {
		wrappedStatement.setPoolable(poolable);
	}

	public boolean isPoolable() throws SQLException {
		return wrappedStatement.isPoolable();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	public String toString()
	{
	    StringBuffer buffer = new StringBuffer();
	    buffer.append(simpleName);
	    buffer.append(":");
	    buffer.append(sql);
	    if (columnIndexes != null)
		{
			buffer.append("{");
			int paramCount = columnIndexes.length;
			for (int i = 0; i < paramCount; i++)
			{
				if (i > 0) buffer.append(",");
				buffer.append("[");
				buffer.append(i);
				buffer.append("]");
				buffer.append(columnIndexes[i]);
			}
			buffer.append("}");
		}
	    if (columnNames != null)
		{
			buffer.append("{");
			int paramCount = columnNames.length;
			for (int i = 0; i < paramCount; i++)
			{
				if (i > 0) buffer.append(",");
				buffer.append("[");
				buffer.append(i);
				buffer.append("]");
				buffer.append(columnNames[i]);
			}
			buffer.append("}");
		}
	    return buffer.toString();
	}

	public void closeOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public boolean isCloseOnCompletion() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}
    
    
}
