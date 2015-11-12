package db.pool;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Suresh Kannan
 * 
 */
public class DBCallableStatement extends DBStatement implements
		CallableStatement {
	private Map<Object, Object> paramMap;
	private Map<Integer, Object> outMap;
	private String sql;

	/**
	 * @param statement
	 */
	protected DBCallableStatement(CallableStatement statement, String sql) {
		super(statement);
		this.sql = sql;
		paramMap = new HashMap<Object, Object>();

		outMap = new HashMap<Integer, Object>();

	}

	protected void addParam(Object index, Object value) {
		paramMap.put(index, value);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#registerOutParameter(int, int)
	 */
	public void registerOutParameter(int parameterIndex, int sqlType)
			throws SQLException {
		outMap.put(parameterIndex, "" + sqlType);
		((CallableStatement) wrappedStatement).registerOutParameter(
				parameterIndex, sqlType);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#registerOutParameter(int, int, int)
	 */
	public void registerOutParameter(int parameterIndex, int sqlType, int scale)
			throws SQLException {
		outMap.put(parameterIndex, "" + sqlType);
		((CallableStatement) wrappedStatement).registerOutParameter(
				parameterIndex, sqlType, scale);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#wasNull()
	 */
	public boolean wasNull() throws SQLException {
		return ((CallableStatement) wrappedStatement).wasNull();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getString(int)
	 */
	public String getString(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement).getString(parameterIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBoolean(int)
	 */
	public boolean getBoolean(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement)
				.getBoolean(parameterIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getByte(int)
	 */
	public byte getByte(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement).getByte(parameterIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getShort(int)
	 */
	public short getShort(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement).getShort(parameterIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getInt(int)
	 */
	public int getInt(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement).getInt(parameterIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getLong(int)
	 */
	public long getLong(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement).getLong(parameterIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getFloat(int)
	 */
	public float getFloat(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement).getFloat(parameterIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getDouble(int)
	 */
	public double getDouble(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement).getDouble(parameterIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBigDecimal(int, int)
	 */
	@Deprecated
	public BigDecimal getBigDecimal(int parameterIndex, int scale)
			throws SQLException {
		return ((CallableStatement) wrappedStatement).getBigDecimal(
				parameterIndex, scale);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBytes(int)
	 */
	public byte[] getBytes(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement).getBytes(parameterIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getDate(int)
	 */
	public Date getDate(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement).getDate(parameterIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getTime(int)
	 */
	public Time getTime(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement).getTime(parameterIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getTimestamp(int)
	 */
	public Timestamp getTimestamp(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement)
				.getTimestamp(parameterIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getObject(int)
	 */
	public Object getObject(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement).getObject(parameterIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBigDecimal(int)
	 */
	public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement)
				.getBigDecimal(parameterIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getObject(int, java.util.Map)
	 */
	public Object getObject(int i, Map<String, Class<?>> map)
			throws SQLException {
		return ((CallableStatement) wrappedStatement).getObject(i, map);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getRef(int)
	 */
	public Ref getRef(int i) throws SQLException {
		return ((CallableStatement) wrappedStatement).getRef(i);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBlob(int)
	 */
	public Blob getBlob(int i) throws SQLException {
		return ((CallableStatement) wrappedStatement).getBlob(i);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getClob(int)
	 */
	public Clob getClob(int i) throws SQLException {
		return ((CallableStatement) wrappedStatement).getClob(i);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getArray(int)
	 */
	public Array getArray(int i) throws SQLException {
		return ((CallableStatement) wrappedStatement).getArray(i);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getDate(int, java.util.Calendar)
	 */
	public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
		return ((CallableStatement) wrappedStatement).getDate(parameterIndex,
				cal);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getTime(int, java.util.Calendar)
	 */
	public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
		return ((CallableStatement) wrappedStatement).getTime(parameterIndex,
				cal);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getTimestamp(int, java.util.Calendar)
	 */
	public Timestamp getTimestamp(int parameterIndex, Calendar cal)
			throws SQLException {
		return ((CallableStatement) wrappedStatement).getTimestamp(
				parameterIndex, cal);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#registerOutParameter(int, int,
	 * java.lang.String)
	 */
	public void registerOutParameter(int paramIndex, int sqlType,
			String typeName) throws SQLException {
		((CallableStatement) wrappedStatement).registerOutParameter(paramIndex,
				sqlType, typeName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String,
	 * int)
	 */
	public void registerOutParameter(String parameterName, int sqlType)
			throws SQLException {
		((CallableStatement) wrappedStatement).registerOutParameter(
				parameterName, sqlType);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String,
	 * int, int)
	 */
	public void registerOutParameter(String parameterName, int sqlType,
			int scale) throws SQLException {
		((CallableStatement) wrappedStatement).registerOutParameter(
				parameterName, sqlType, scale);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String,
	 * int, java.lang.String)
	 */
	public void registerOutParameter(String parameterName, int sqlType,
			String typeName) throws SQLException {
		((CallableStatement) wrappedStatement).registerOutParameter(
				parameterName, sqlType, typeName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getURL(int)
	 */
	public URL getURL(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement).getURL(parameterIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setURL(java.lang.String, java.net.URL)
	 */
	public void setURL(String parameterName, URL val) throws SQLException {
		((CallableStatement) wrappedStatement).setURL(parameterName, val);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setNull(java.lang.String, int)
	 */
	public void setNull(String parameterName, int sqlType) throws SQLException {
		((CallableStatement) wrappedStatement).setNull(parameterName, sqlType);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setBoolean(java.lang.String, boolean)
	 */
	public void setBoolean(String parameterName, boolean x) throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setBoolean(parameterName, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setByte(java.lang.String, byte)
	 */
	public void setByte(String parameterName, byte x) throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setByte(parameterName, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setShort(java.lang.String, short)
	 */
	public void setShort(String parameterName, short x) throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setShort(parameterName, x);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setInt(java.lang.String, int)
	 */
	public void setInt(String parameterName, int x) throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setInt(parameterName, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setLong(java.lang.String, long)
	 */
	public void setLong(String parameterName, long x) throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setLong(parameterName, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setFloat(java.lang.String, float)
	 */
	public void setFloat(String parameterName, float x) throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setFloat(parameterName, x);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setDouble(java.lang.String, double)
	 */
	public void setDouble(String parameterName, double x) throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setDouble(parameterName, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setBigDecimal(java.lang.String,
	 * java.math.BigDecimal)
	 */
	public void setBigDecimal(String parameterName, BigDecimal x)
			throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setBigDecimal(parameterName, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setString(java.lang.String,
	 * java.lang.String)
	 */
	public void setString(String parameterName, String x) throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setString(parameterName, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setBytes(java.lang.String, byte[])
	 */
	public void setBytes(String parameterName, byte[] x) throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setBytes(parameterName, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setDate(java.lang.String, java.sql.Date)
	 */
	public void setDate(String parameterName, Date x) throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setDate(parameterName, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setTime(java.lang.String, java.sql.Time)
	 */
	public void setTime(String parameterName, Time x) throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setTime(parameterName, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setTimestamp(java.lang.String,
	 * java.sql.Timestamp)
	 */
	public void setTimestamp(String parameterName, Timestamp x)
			throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setTimestamp(parameterName, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setAsciiStream(java.lang.String,
	 * java.io.InputStream, int)
	 */
	public void setAsciiStream(String parameterName, InputStream x, int length)
			throws SQLException {
		addParam(parameterName, "<InputStream>");
		((CallableStatement) wrappedStatement).setAsciiStream(parameterName, x,
				length);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setBinaryStream(java.lang.String,
	 * java.io.InputStream, int)
	 */
	public void setBinaryStream(String parameterName, InputStream x, int length)
			throws SQLException {
		addParam(parameterName, "<BinaryStream>");
		((CallableStatement) wrappedStatement).setBinaryStream(parameterName,
				x, length);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setObject(java.lang.String,
	 * java.lang.Object, int, int)
	 */
	public void setObject(String parameterName, Object x, int targetSqlType,
			int scale) throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setObject(parameterName, x,
				targetSqlType, scale);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setObject(java.lang.String,
	 * java.lang.Object, int)
	 */
	public void setObject(String parameterName, Object x, int targetSqlType)
			throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setObject(parameterName, x,
				targetSqlType);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setObject(java.lang.String,
	 * java.lang.Object)
	 */
	public void setObject(String parameterName, Object x) throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setObject(parameterName, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setCharacterStream(java.lang.String,
	 * java.io.Reader, int)
	 */
	public void setCharacterStream(String parameterName, Reader reader,
			int length) throws SQLException {
		addParam(parameterName, "<CharacterStream>");
		((CallableStatement) wrappedStatement).setCharacterStream(
				parameterName, reader, length);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setDate(java.lang.String, java.sql.Date,
	 * java.util.Calendar)
	 */
	public void setDate(String parameterName, Date x, Calendar cal)
			throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setDate(parameterName, x, cal);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setTime(java.lang.String, java.sql.Time,
	 * java.util.Calendar)
	 */
	public void setTime(String parameterName, Time x, Calendar cal)
			throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setTime(parameterName, x, cal);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setTimestamp(java.lang.String,
	 * java.sql.Timestamp, java.util.Calendar)
	 */
	public void setTimestamp(String parameterName, Timestamp x, Calendar cal)
			throws SQLException {
		addParam(parameterName, x);
		((CallableStatement) wrappedStatement).setTimestamp(parameterName, x,
				cal);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setNull(java.lang.String, int,
	 * java.lang.String)
	 */
	public void setNull(String parameterName, int sqlType, String typeName)
			throws SQLException {
		addParam(parameterName, "null");
		((CallableStatement) wrappedStatement).setNull(parameterName, sqlType,
				typeName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getString(java.lang.String)
	 */
	public String getString(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getString(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBoolean(java.lang.String)
	 */
	public boolean getBoolean(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getBoolean(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getByte(java.lang.String)
	 */
	public byte getByte(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getByte(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getShort(java.lang.String)
	 */
	public short getShort(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getShort(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getInt(java.lang.String)
	 */
	public int getInt(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getInt(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getLong(java.lang.String)
	 */
	public long getLong(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getLong(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getFloat(java.lang.String)
	 */
	public float getFloat(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getFloat(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getDouble(java.lang.String)
	 */
	public double getDouble(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getDouble(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBytes(java.lang.String)
	 */
	public byte[] getBytes(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getBytes(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getDate(java.lang.String)
	 */
	public Date getDate(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getDate(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getTime(java.lang.String)
	 */
	public Time getTime(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getTime(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getTimestamp(java.lang.String)
	 */
	public Timestamp getTimestamp(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement)
				.getTimestamp(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getObject(java.lang.String)
	 */
	public Object getObject(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getObject(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBigDecimal(java.lang.String)
	 */
	public BigDecimal getBigDecimal(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement)
				.getBigDecimal(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getObject(java.lang.String,
	 * java.util.Map)
	 */
	public Object getObject(String parameterName, Map<String, Class<?>> map)
			throws SQLException {
		return ((CallableStatement) wrappedStatement).getObject(parameterName,
				map);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getRef(java.lang.String)
	 */
	public Ref getRef(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getRef(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBlob(java.lang.String)
	 */
	public Blob getBlob(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getBlob(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getClob(java.lang.String)
	 */
	public Clob getClob(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getClob(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getArray(java.lang.String)
	 */
	public Array getArray(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getArray(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getDate(java.lang.String,
	 * java.util.Calendar)
	 */
	public Date getDate(String parameterName, Calendar cal) throws SQLException {
		return ((CallableStatement) wrappedStatement).getDate(parameterName,
				cal);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getTime(java.lang.String,
	 * java.util.Calendar)
	 */
	public Time getTime(String parameterName, Calendar cal) throws SQLException {
		return ((CallableStatement) wrappedStatement).getTime(parameterName,
				cal);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getTimestamp(java.lang.String,
	 * java.util.Calendar)
	 */
	public Timestamp getTimestamp(String parameterName, Calendar cal)
			throws SQLException {
		return ((CallableStatement) wrappedStatement).getTimestamp(
				parameterName, cal);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getURL(java.lang.String)
	 */
	public URL getURL(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getURL(parameterName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#executeQuery()
	 */
	public ResultSet executeQuery() throws SQLException {
		return add(((CallableStatement) wrappedStatement).executeQuery());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#executeUpdate()
	 */
	public int executeUpdate() throws SQLException {
		return ((CallableStatement) wrappedStatement).executeUpdate();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setNull(int, int)
	 */
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		((CallableStatement) wrappedStatement).setNull(parameterIndex, sqlType);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setBoolean(int, boolean)
	 */
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		((CallableStatement) wrappedStatement).setBoolean(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setByte(int, byte)
	 */
	public void setByte(int parameterIndex, byte x) throws SQLException {
		((CallableStatement) wrappedStatement).setByte(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setShort(int, short)
	 */
	public void setShort(int parameterIndex, short x) throws SQLException {
		((CallableStatement) wrappedStatement).setShort(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setInt(int, int)
	 */
	public void setInt(int parameterIndex, int x) throws SQLException {
		addParam(parameterIndex, x);
		((CallableStatement) wrappedStatement).setInt(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setLong(int, long)
	 */
	public void setLong(int parameterIndex, long x) throws SQLException {
		addParam(parameterIndex, x);
		((CallableStatement) wrappedStatement).setLong(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setFloat(int, float)
	 */
	public void setFloat(int parameterIndex, float x) throws SQLException {
		addParam(parameterIndex, x);
		((CallableStatement) wrappedStatement).setFloat(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setDouble(int, double)
	 */
	public void setDouble(int parameterIndex, double x) throws SQLException {
		addParam(parameterIndex, x);
		((CallableStatement) wrappedStatement).setDouble(parameterIndex, x);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setBigDecimal(int, java.math.BigDecimal)
	 */
	public void setBigDecimal(int parameterIndex, BigDecimal x)
			throws SQLException {
		addParam(parameterIndex, x);
		((CallableStatement) wrappedStatement).setBigDecimal(parameterIndex, x);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setString(int, java.lang.String)
	 */
	public void setString(int parameterIndex, String x) throws SQLException {
		addParam(parameterIndex, x);
		((CallableStatement) wrappedStatement).setString(parameterIndex, x);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setBytes(int, byte[])
	 */
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		addParam(parameterIndex, x);
		((CallableStatement) wrappedStatement).setBytes(parameterIndex, x);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setDate(int, java.sql.Date)
	 */
	public void setDate(int parameterIndex, Date x) throws SQLException {
		addParam(parameterIndex, x);
		((CallableStatement) wrappedStatement).setDate(parameterIndex, x);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setTime(int, java.sql.Time)
	 */
	public void setTime(int parameterIndex, Time x) throws SQLException {
		addParam(parameterIndex, x);
		((CallableStatement) wrappedStatement).setTime(parameterIndex, x);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp)
	 */
	public void setTimestamp(int parameterIndex, Timestamp x)
			throws SQLException {
		addParam(parameterIndex, x);
		((CallableStatement) wrappedStatement).setTimestamp(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream,
	 * int)
	 */
	public void setAsciiStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		addParam(parameterIndex, "<Inputstream>");
		((CallableStatement) wrappedStatement).setAsciiStream(parameterIndex,
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
		((CallableStatement) wrappedStatement).setUnicodeStream(parameterIndex,
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
		addParam(parameterIndex, "<BinaryStream>");
		((CallableStatement) wrappedStatement).setBinaryStream(parameterIndex,
				x, length);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#clearParameters()
	 */
	public void clearParameters() throws SQLException {
		paramMap.clear();
		outMap.clear();
		((CallableStatement) wrappedStatement).clearParameters();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int,
	 * int)
	 */
	public void setObject(int parameterIndex, Object x, int targetSqlType,
			int scale) throws SQLException {
		addParam(parameterIndex, x);
		((CallableStatement) wrappedStatement).setObject(parameterIndex, x,
				targetSqlType, scale);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int)
	 */
	public void setObject(int parameterIndex, Object x, int targetSqlType)
			throws SQLException {
		addParam(parameterIndex, x);
		((CallableStatement) wrappedStatement).setObject(parameterIndex, x,
				targetSqlType);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object)
	 */
	public void setObject(int parameterIndex, Object x) throws SQLException {
		addParam(parameterIndex, x);
		((CallableStatement) wrappedStatement).setObject(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#execute()
	 */
	public boolean execute() throws SQLException {
		return ((CallableStatement) wrappedStatement).execute();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#addBatch()
	 */
	public void addBatch() throws SQLException {
		((CallableStatement) wrappedStatement).addBatch();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader,
	 * int)
	 */
	public void setCharacterStream(int parameterIndex, Reader reader, int length)
			throws SQLException {
		((CallableStatement) wrappedStatement).setCharacterStream(
				parameterIndex, reader, length);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setRef(int, java.sql.Ref)
	 */
	public void setRef(int i, Ref x) throws SQLException {
		((CallableStatement) wrappedStatement).setRef(i, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setBlob(int, java.sql.Blob)
	 */
	public void setBlob(int i, Blob x) throws SQLException {
		((CallableStatement) wrappedStatement).setBlob(i, x);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setClob(int, java.sql.Clob)
	 */
	public void setClob(int i, Clob x) throws SQLException {
		((CallableStatement) wrappedStatement).setClob(i, x);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setArray(int, java.sql.Array)
	 */
	public void setArray(int i, Array x) throws SQLException {
		((CallableStatement) wrappedStatement).setArray(i, x);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#getMetaData()
	 */
	public ResultSetMetaData getMetaData() throws SQLException {
		return ((CallableStatement) wrappedStatement).getMetaData();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setDate(int, java.sql.Date,
	 * java.util.Calendar)
	 */
	public void setDate(int parameterIndex, Date x, Calendar cal)
			throws SQLException {
		((CallableStatement) wrappedStatement).setDate(parameterIndex, x, cal);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setTime(int, java.sql.Time,
	 * java.util.Calendar)
	 */
	public void setTime(int parameterIndex, Time x, Calendar cal)
			throws SQLException {
		((CallableStatement) wrappedStatement).setTime(parameterIndex, x, cal);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp,
	 * java.util.Calendar)
	 */
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
			throws SQLException {
		((CallableStatement) wrappedStatement).setTimestamp(parameterIndex, x,
				cal);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setNull(int, int, java.lang.String)
	 */
	public void setNull(int paramIndex, int sqlType, String typeName)
			throws SQLException {
		((CallableStatement) wrappedStatement).setNull(paramIndex, sqlType,
				typeName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setURL(int, java.net.URL)
	 */
	public void setURL(int parameterIndex, URL x) throws SQLException {
		((CallableStatement) wrappedStatement).setURL(parameterIndex, x);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#getParameterMetaData()
	 */
	public ParameterMetaData getParameterMetaData() throws SQLException {
		return ((CallableStatement) wrappedStatement).getParameterMetaData();
	}

	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		((CallableStatement) wrappedStatement).setRowId(parameterIndex, x);
	}

	public void setNString(int parameterIndex, String value)
			throws SQLException {
		((CallableStatement) wrappedStatement)
				.setNString(parameterIndex, value);

	}

	public void setNCharacterStream(int parameterIndex, Reader value,
			long length) throws SQLException {
		((CallableStatement) wrappedStatement).setNCharacterStream(
				parameterIndex, value, length);

	}

	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		((CallableStatement) wrappedStatement).setNClob(parameterIndex, value);
	}

	public void setClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		((CallableStatement) wrappedStatement).setClob(parameterIndex, reader,
				length);

	}

	public void setBlob(int parameterIndex, InputStream inputStream, long length)
			throws SQLException {
		((CallableStatement) wrappedStatement).setBlob(parameterIndex,
				inputStream, length);
	}

	public void setNClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		((CallableStatement) wrappedStatement).setNClob(parameterIndex, reader,
				length);
	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		((CallableStatement) wrappedStatement).setSQLXML(parameterIndex,
				xmlObject);
	}

	public void setAsciiStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		((CallableStatement) wrappedStatement)
				.setAsciiStream(parameterIndex, x);
	}

	public void setBinaryStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		((CallableStatement) wrappedStatement).setBinaryStream(parameterIndex,
				x, length);
	}

	public void setCharacterStream(int parameterIndex, Reader reader,
			long length) throws SQLException {
		((CallableStatement) wrappedStatement).setCharacterStream(
				parameterIndex, reader, length);
	}

	public void setAsciiStream(int parameterIndex, InputStream x)
			throws SQLException {
		((CallableStatement) wrappedStatement)
				.setAsciiStream(parameterIndex, x);

	}

	public void setBinaryStream(int parameterIndex, InputStream x)
			throws SQLException {
		((CallableStatement) wrappedStatement).setBinaryStream(parameterIndex,
				x);
	}

	public void setCharacterStream(int parameterIndex, Reader reader)
			throws SQLException {
		((CallableStatement) wrappedStatement).setCharacterStream(
				parameterIndex, reader);
	}

	public void setNCharacterStream(int parameterIndex, Reader value)
			throws SQLException {
		((CallableStatement) wrappedStatement).setNCharacterStream(
				parameterIndex, value);
	}

	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		((CallableStatement) wrappedStatement).setClob(parameterIndex, reader);
	}

	public void setBlob(int parameterIndex, InputStream inputStream)
			throws SQLException {
		((CallableStatement) wrappedStatement).setBlob(parameterIndex,
				inputStream);

	}

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		((CallableStatement) wrappedStatement).setNClob(parameterIndex, reader);
	}

	public RowId getRowId(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement).getRowId(parameterIndex);
	}

	public RowId getRowId(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getRowId(parameterName);
	}

	public void setRowId(String parameterName, RowId x) throws SQLException {
		((CallableStatement) wrappedStatement).setRowId(parameterName, x);
	}

	public void setNString(String parameterName, String value)
			throws SQLException {
		((CallableStatement) wrappedStatement).setNString(parameterName, value);
	}

	public void setNCharacterStream(String parameterName, Reader value,
			long length) throws SQLException {
		((CallableStatement) wrappedStatement).setNCharacterStream(
				parameterName, value, length);
	}

	public void setNClob(String parameterName, NClob value) throws SQLException {
		((CallableStatement) wrappedStatement).setNClob(parameterName, value);
	}

	public void setClob(String parameterName, Reader reader, long length)
			throws SQLException {
		((CallableStatement) wrappedStatement).setClob(parameterName, reader);
	}

	public void setBlob(String parameterName, InputStream inputStream,
			long length) throws SQLException {
		((CallableStatement) wrappedStatement).setBlob(parameterName,
				inputStream, length);
	}

	public void setNClob(String parameterName, Reader reader, long length)
			throws SQLException {
		((CallableStatement) wrappedStatement).setNClob(parameterName, reader,
				length);
	}

	public NClob getNClob(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement).getNClob(parameterIndex);
	}

	public NClob getNClob(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getNClob(parameterName);
	}

	public void setSQLXML(String parameterName, SQLXML xmlObject)
			throws SQLException {
		((CallableStatement) wrappedStatement).setSQLXML(parameterName,
				xmlObject);

	}

	public SQLXML getSQLXML(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement).getSQLXML(parameterIndex);
	}

	public SQLXML getSQLXML(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getSQLXML(parameterName);
	}

	public String getNString(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement)
				.getNString(parameterIndex);
	}

	public String getNString(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement).getNString(parameterName);
	}

	public Reader getNCharacterStream(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement)
				.getNCharacterStream(parameterIndex);
	}

	public Reader getNCharacterStream(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement)
				.getNCharacterStream(parameterName);
	}

	public Reader getCharacterStream(int parameterIndex) throws SQLException {
		return ((CallableStatement) wrappedStatement)
				.getCharacterStream(parameterIndex);
	}

	public Reader getCharacterStream(String parameterName) throws SQLException {
		return ((CallableStatement) wrappedStatement)
				.getCharacterStream(parameterName);
	}

	public void setBlob(String parameterName, Blob x) throws SQLException {
		((CallableStatement) wrappedStatement).setBlob(parameterName, x);
	}

	public void setClob(String parameterName, Clob x) throws SQLException {
		((CallableStatement) wrappedStatement).setClob(parameterName, x);
	}

	public void setAsciiStream(String parameterName, InputStream x, long length)
			throws SQLException {
		((CallableStatement) wrappedStatement).setAsciiStream(parameterName, x,
				length);
	}

	public void setBinaryStream(String parameterName, InputStream x, long length)
			throws SQLException {
		((CallableStatement) wrappedStatement)
				.setBinaryStream(parameterName, x);
	}

	public void setCharacterStream(String parameterName, Reader reader,
			long length) throws SQLException {
		((CallableStatement) wrappedStatement).setCharacterStream(
				parameterName, reader, length);
	}

	public void setAsciiStream(String parameterName, InputStream x)
			throws SQLException {
		((CallableStatement) wrappedStatement).setAsciiStream(parameterName, x);
	}

	public void setBinaryStream(String parameterName, InputStream x)
			throws SQLException {
		((CallableStatement) wrappedStatement)
				.setBinaryStream(parameterName, x);
	}

	public void setCharacterStream(String parameterName, Reader reader)
			throws SQLException {
		((CallableStatement) wrappedStatement).setCharacterStream(
				parameterName, reader);
	}

	public void setNCharacterStream(String parameterName, Reader value)
			throws SQLException {
		((CallableStatement) wrappedStatement).setNCharacterStream(
				parameterName, value);
	}

	public void setClob(String parameterName, Reader reader)
			throws SQLException {
		((CallableStatement) wrappedStatement).setClob(parameterName, reader);
	}

	public void setBlob(String parameterName, InputStream inputStream)
			throws SQLException {
		((CallableStatement) wrappedStatement).setBlob(parameterName,
				inputStream);
	}

	public void setNClob(String parameterName, Reader reader)
			throws SQLException {
		((CallableStatement) wrappedStatement).setNClob(parameterName, reader);
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(sql);
		buffer.append("{");
		buffer.append("OutParameters:");
		Iterator<Integer> outValIter = outMap.keySet().iterator();
		while (outValIter.hasNext()) {
			Integer i = outValIter.next();
			buffer.append(i.intValue() + "=" + outMap.get(i));
		}
		buffer.append("\nOtherParameters:");
		Iterator<Object> paramValIter = paramMap.keySet().iterator();
		while (paramValIter.hasNext()) {
			Object i = paramValIter.next();
			if (i instanceof Integer)
				buffer.append(((Integer) i).intValue() + "=" + paramMap.get(i));
			else
				buffer.append(i + "=" + paramMap.get(i));
			buffer.append(",");
		}
		buffer.append("}");
		return buffer.toString();
	}

	@Override
	public void close() throws SQLException {
		paramMap.clear();
		outMap.clear();
		super.close();
	}

	public void closeOnCompletion() throws SQLException {
	}

	public boolean isCloseOnCompletion() throws SQLException {
		return false;
	}

	public <T> T getObject(int parameterIndex, Class<T> type)
			throws SQLException {
		return null;
	}

	public <T> T getObject(String parameterName, Class<T> type)
			throws SQLException {
		return null;
	}
}
