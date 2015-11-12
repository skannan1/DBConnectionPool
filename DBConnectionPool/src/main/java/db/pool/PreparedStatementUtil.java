/**
 * 
 */
package db.pool;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * This is an utility class for facilities when setting prepared statement
 * parameters.
 * 
 * @author lgobi
 * 
 */
public class PreparedStatementUtil
{

	/**
	 * Sets the prepared statement parameter of type <code>Timestamp</code>.
	 * 
	 * @param prst
	 *            the prepared statement instance to be used.
	 * @param parameterIndex
	 *            the parameter index to be set
	 * @param value
	 *            the value to be set
	 * @throws SQLException
	 *             if any error occurs.
	 */
	public static void set(PreparedStatement prst, int parameterIndex,
			Timestamp value) throws SQLException
	{
		if (prst == null) return;

		if (value == null) prst.setNull(parameterIndex,
				java.sql.Types.TIMESTAMP);
		else prst.setTimestamp(parameterIndex, value);
	}

	/**
	 * Sets the prepared statement parameter of type <code>String</code>.
	 * 
	 * @param prst
	 *            the prepared statement instance to be used.
	 * @param parameterIndex
	 *            the parameter index to be set
	 * @param value
	 *            the value to be set
	 * @throws SQLException
	 *             if any error occurs.
	 */
	public static void set(PreparedStatement prst, int parameterIndex,
			String value) throws SQLException
	{
		if (prst == null) return;

		if (value == null) prst.setNull(parameterIndex, java.sql.Types.VARCHAR);
		else prst.setString(parameterIndex, value);
	}

}
