/*
 * Created on Aug 10, 2009
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package db.pool;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a wrapper class created for audit purpose as well to control the
 * result set close when this statement is close.
 * 
 * 
 * @author Suresh Kannan
 * 
 * 
 */
public class DBPreparedStatement extends DBStatement implements
		PreparedStatement {

	private String sql;
	private Map<Integer, Object> paramMap;
	private List<Map<Integer, Object>> batchList = null;
	private int sqlParamCount;

	/**
	 * @param statement
	 */
	protected DBPreparedStatement(Statement statement, String sql) {
		super(statement);

		this.sql = sql;
		sqlParamCount = countParameters(sql);
		paramMap = new HashMap<Integer, Object>(sqlParamCount);
	}

	protected int countParameters(String sql) {
		int paramCount = 0;
		char[] c = sql.toCharArray();
		for (int i = 0; i < c.length; i++) {
			if (c[i] == '?' || c[i] == ':')
				paramCount++;
		}
		return paramCount;
	}

	protected void addParam(int index, Object value) {
		paramMap.put(IntegerFactory.get(index), value);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(sql);
		if (batchList != null) {
			for (Map<Integer, Object> parameters : batchList)
				printParameters(buffer, parameters);
		} else
			printParameters(buffer, paramMap);

		return buffer.toString();
	}

	private void printParameters(StringBuffer buffer,
			Map<Integer, Object> parameters) {
		buffer.append("\n{");
		int paramCount = parameters.size();
		for (int i = 0; i < paramCount; i++) {
			if (i > 0)
				buffer.append(",");
			buffer.append("[");
			buffer.append(i);
			buffer.append("]");
			Object key = IntegerFactory.get(i + 1);
			buffer.append(parameters.get(key));
		}
		buffer.append("}");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#close()
	 */
	public void close() throws SQLException {
		paramMap.clear();
		super.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#cancel()
	 */
	public void cancel() throws SQLException {
		paramMap.clear();
		super.cancel();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#clearBatch()
	 */
	public void clearBatch() throws SQLException {
		paramMap.clear();
		super.clearBatch();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#executeQuery()
	 */
	public ResultSet executeQuery() throws SQLException {
		return add(((PreparedStatement) wrappedStatement).executeQuery());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#executeUpdate()
	 */
	public int executeUpdate() throws SQLException {
		return ((PreparedStatement) wrappedStatement).executeUpdate();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setNull(int, int)
	 */
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		addParam(parameterIndex, "null");
		((PreparedStatement) wrappedStatement).setNull(parameterIndex, sqlType);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setBoolean(int, boolean)
	 */
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		addParam(parameterIndex, Boolean.valueOf(x));
		((PreparedStatement) wrappedStatement).setBoolean(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setByte(int, byte)
	 */
	public void setByte(int parameterIndex, byte x) throws SQLException {
		addParam(parameterIndex, Byte.toString(x));
		((PreparedStatement) wrappedStatement).setByte(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setShort(int, short)
	 */
	public void setShort(int parameterIndex, short x) throws SQLException {
		addParam(parameterIndex, Short.toString(x));
		((PreparedStatement) wrappedStatement).setShort(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setInt(int, int)
	 */
	public void setInt(int parameterIndex, int x) throws SQLException {
		addParam(parameterIndex, Integer.toString(x));
		((PreparedStatement) wrappedStatement).setInt(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setLong(int, long)
	 */
	public void setLong(int parameterIndex, long x) throws SQLException {
		addParam(parameterIndex, Long.toString(x));
		((PreparedStatement) wrappedStatement).setLong(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setFloat(int, float)
	 */
	public void setFloat(int parameterIndex, float x) throws SQLException {
		addParam(parameterIndex, Float.toString(x));
		((PreparedStatement) wrappedStatement).setFloat(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setDouble(int, double)
	 */
	public void setDouble(int parameterIndex, double x) throws SQLException {
		addParam(parameterIndex, Double.toString(x));
		((PreparedStatement) wrappedStatement).setDouble(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setBigDecimal(int, java.math.BigDecimal)
	 */
	public void setBigDecimal(int parameterIndex, BigDecimal x)
			throws SQLException {
		addParam(parameterIndex, x == null ? "null" : x.toString());
		((PreparedStatement) wrappedStatement).setBigDecimal(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setString(int, java.lang.String)
	 */
	public void setString(int parameterIndex, String x) throws SQLException {
		addParam(parameterIndex, x);
		((PreparedStatement) wrappedStatement).setString(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setBytes(int, byte[])
	 */
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		addParam(parameterIndex, String.valueOf(x));
		((PreparedStatement) wrappedStatement).setBytes(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setDate(int, java.sql.Date)
	 */
	public void setDate(int parameterIndex, Date x) throws SQLException {
		addParam(parameterIndex, x == null ? "null" : x.toString());
		((PreparedStatement) wrappedStatement).setDate(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setTime(int, java.sql.Time)
	 */
	public void setTime(int parameterIndex, Time x) throws SQLException {
		addParam(parameterIndex, x == null ? "null" : x.toString());
		((PreparedStatement) wrappedStatement).setTime(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp)
	 */
	public void setTimestamp(int parameterIndex, Timestamp x)
			throws SQLException {
		addParam(parameterIndex, x == null ? "null" : x.toString());
		((PreparedStatement) wrappedStatement).setTimestamp(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream,
	 * int)
	 */
	public void setAsciiStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		addParam(parameterIndex, "AsciiStream[" + length + "");
		((PreparedStatement) wrappedStatement).setAsciiStream(parameterIndex,
				x, length);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setUnicodeStream(int,
	 * java.io.InputStream, int)
	 */
	@Deprecated
	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		addParam(parameterIndex, "UnicodeStream[" + length + "");
		((PreparedStatement) wrappedStatement).setUnicodeStream(parameterIndex,
				x, length);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream,
	 * int)
	 */
	public void setBinaryStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		addParam(parameterIndex, "BinaryStream[" + length + "");
		((PreparedStatement) wrappedStatement).setBinaryStream(parameterIndex,
				x, length);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#clearParameters()
	 */
	public void clearParameters() throws SQLException {
		paramMap.clear();
		((PreparedStatement) wrappedStatement).clearParameters();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int,
	 * int)
	 */
	public void setObject(int parameterIndex, Object x, int targetSqlType,
			int scale) throws SQLException {
		addParam(parameterIndex, x == null ? "null" : x.toString());
		((PreparedStatement) wrappedStatement).setObject(parameterIndex, x,
				targetSqlType);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int)
	 */
	public void setObject(int parameterIndex, Object x, int targetSqlType)
			throws SQLException {
		addParam(parameterIndex, x == null ? "null" : x.toString());
		((PreparedStatement) wrappedStatement).setObject(parameterIndex, x,
				targetSqlType);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object)
	 */
	public void setObject(int parameterIndex, Object x) throws SQLException {
		addParam(parameterIndex, x == null ? "null" : x.toString());
		((PreparedStatement) wrappedStatement).setObject(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#execute()
	 */
	public boolean execute() throws SQLException {
		return ((PreparedStatement) wrappedStatement).execute();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#addBatch()
	 */
	public void addBatch() throws SQLException {
		if (batchList == null)
			batchList = new ArrayList<Map<Integer, Object>>();

		batchList.add(paramMap);
		paramMap = new HashMap<Integer, Object>(sqlParamCount);
		((PreparedStatement) wrappedStatement).addBatch();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader,
	 * int)
	 */
	public void setCharacterStream(int parameterIndex, Reader reader, int length)
			throws SQLException {
		addParam(parameterIndex, "CharacterStream[" + length + "");
		((PreparedStatement) wrappedStatement).setCharacterStream(
				parameterIndex, reader, length);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setRef(int, java.sql.Ref)
	 */
	public void setRef(int i, Ref x) throws SQLException {
		addParam(i, x == null ? "null" : x.toString());
		((PreparedStatement) wrappedStatement).setRef(i, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setBlob(int, java.sql.Blob)
	 */
	public void setBlob(int i, Blob x) throws SQLException {
		addParam(i, "<Blob>");
		((PreparedStatement) wrappedStatement).setBlob(i, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setClob(int, java.sql.Clob)
	 */
	public void setClob(int i, Clob x) throws SQLException {
		addParam(i, "<Clob>");
		((PreparedStatement) wrappedStatement).setClob(i, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setArray(int, java.sql.Array)
	 */
	public void setArray(int i, Array x) throws SQLException {
		addParam(i, x == null ? "null" : x.toString());
		((PreparedStatement) wrappedStatement).setArray(i, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#getMetaData()
	 */
	public ResultSetMetaData getMetaData() throws SQLException {
		return ((PreparedStatement) wrappedStatement).getMetaData();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setDate(int, java.sql.Date,
	 * java.util.Calendar)
	 */
	public void setDate(int parameterIndex, Date x, Calendar cal)
			throws SQLException {
		addParam(parameterIndex, x == null ? "null" : x.toString());
		((PreparedStatement) wrappedStatement).setDate(parameterIndex, x, cal);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setTime(int, java.sql.Time,
	 * java.util.Calendar)
	 */
	public void setTime(int parameterIndex, Time x, Calendar cal)
			throws SQLException {
		addParam(parameterIndex, x == null ? "null" : x.toString());
		((PreparedStatement) wrappedStatement).setTime(parameterIndex, x, cal);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp,
	 * java.util.Calendar)
	 */
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
			throws SQLException {
		addParam(parameterIndex, x == null ? "null" : x.toString());
		((PreparedStatement) wrappedStatement).setTimestamp(parameterIndex, x,
				cal);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setNull(int, int, java.lang.String)
	 */
	public void setNull(int paramIndex, int sqlType, String typeName)
			throws SQLException {
		addParam(paramIndex, "null");
		((PreparedStatement) wrappedStatement).setNull(paramIndex, sqlType,
				typeName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setURL(int, java.net.URL)
	 */
	public void setURL(int parameterIndex, URL x) throws SQLException {
		addParam(parameterIndex, x == null ? "null" : x.toString());
		((PreparedStatement) wrappedStatement).setURL(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#getParameterMetaData()
	 */
	public ParameterMetaData getParameterMetaData() throws SQLException {
		return ((PreparedStatement) wrappedStatement).getParameterMetaData();
	}

	public boolean isClosed() throws SQLException {
		return ((PreparedStatement) wrappedStatement).isClosed();
	}

	public void setPoolable(boolean poolable) throws SQLException {
		((PreparedStatement) wrappedStatement).isPoolable();
	}

	public boolean isPoolable() throws SQLException {
		return ((PreparedStatement) wrappedStatement).isPoolable();
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		return ((PreparedStatement) wrappedStatement).unwrap(iface);
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return ((PreparedStatement) wrappedStatement).isWrapperFor(iface);
	}

	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		((PreparedStatement) wrappedStatement).setRowId(parameterIndex, x);
	}

	public void setNString(int parameterIndex, String value)
			throws SQLException {
		((PreparedStatement) wrappedStatement)
				.setNString(parameterIndex, value);
	}

	public void setNCharacterStream(int parameterIndex, Reader value,
			long length) throws SQLException {
		((PreparedStatement) wrappedStatement).setNCharacterStream(
				parameterIndex, value, length);
	}

	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		((PreparedStatement) wrappedStatement).setNClob(parameterIndex, value);
	}

	public void setClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		((PreparedStatement) wrappedStatement).setClob(parameterIndex, reader,
				length);
	}

	public void setBlob(int parameterIndex, InputStream inputStream, long length)
			throws SQLException {
		((PreparedStatement) wrappedStatement).setBlob(parameterIndex,
				inputStream, length);
	}

	public void setNClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		((PreparedStatement) wrappedStatement).setNClob(parameterIndex, reader,
				length);
	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		((PreparedStatement) wrappedStatement).setSQLXML(parameterIndex,
				xmlObject);

	}

	public void setAsciiStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		((PreparedStatement) wrappedStatement).setAsciiStream(parameterIndex,
				x, length);

	}

	public void setBinaryStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		((PreparedStatement) wrappedStatement).setBinaryStream(parameterIndex,
				x, length);
	}

	public void setCharacterStream(int parameterIndex, Reader reader,
			long length) throws SQLException {
		((PreparedStatement) wrappedStatement).setCharacterStream(
				parameterIndex, reader, length);
	}

	public void setAsciiStream(int parameterIndex, InputStream x)
			throws SQLException {
		((PreparedStatement) wrappedStatement)
				.setAsciiStream(parameterIndex, x);
	}

	public void setBinaryStream(int parameterIndex, InputStream x)
			throws SQLException {
		((PreparedStatement) wrappedStatement).setBinaryStream(parameterIndex,
				x);
	}

	public void setCharacterStream(int parameterIndex, Reader reader)
			throws SQLException {
		((PreparedStatement) wrappedStatement).setCharacterStream(
				parameterIndex, reader);
	}

	public void setNCharacterStream(int parameterIndex, Reader value)
			throws SQLException {
		((PreparedStatement) wrappedStatement).setNCharacterStream(
				parameterIndex, value);
	}

	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		((PreparedStatement) wrappedStatement).setClob(parameterIndex, reader);
	}

	public void setBlob(int parameterIndex, InputStream inputStream)
			throws SQLException {
		((PreparedStatement) wrappedStatement).setBlob(parameterIndex,
				inputStream);
	}

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		((PreparedStatement) wrappedStatement).setNClob(parameterIndex, reader);
	}

}
