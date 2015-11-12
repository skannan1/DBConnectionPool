package db.pool;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is a wrapper for {@link java.sql.PreparedStatement} and provides the ability to do named binds on SQL variables that matches the format: ':(\w+)'<br>
 * <br>
 * <b>Examples:</b><br>
 * :var, :my_var, :var2 <br>
 * <br>
 * <b>Important:</b><br>
 * You should not have any string on your SQL that matches the pattern above and is not supposed to be a variable. Like <b>SELECT "this is not a <font color='red'>:variable</font>" FROM...</b><br>
 * this will break the index and give you unwanted results while binding values.
 */
public class ParameterBinder
{

    private static final String REGEX_PATTERN = "\\:[\\w]*(?=([^'\\\\]*(\\\\.|'([^'\\\\]*\\\\.)*[^'\\\\]*'))*[^']*$)";
    private PreparedStatement ps = null;
    private Hashtable<String, ArrayList<Integer>> bindTable = new Hashtable<String, ArrayList<Integer>>();

    /**
     * Binds a String value to the specified variable name
     * 
     * @param parameter
     *            The SQL variable name
     * @param value
     *            The String value being binded
     * @throws SQLException
     */
    public void setString(String parameter, String value) throws SQLException
    {
	if (!bindTable.containsKey(parameter))
	    throw new SQLException("Parameter '" + parameter + "' not found.");

	for (int index : bindTable.get(parameter))
	    ps.setString(index, value);

    }

    /**
     * Binds a ArrayList<String> values to the specified variable suffix Is expected that the query have parameters in the format <parameterSuffix>_<1 based index> like ':var_1, :var_2, :var_3 ...'
     * 
     * @param parameterSuffix
     *            The SQL variable name
     * @param valueList
     *            The String value being binded *
     * @param maxValue
     *            The maximum value for the index
     * @throws SQLException
     */
    public void setStringArray(String parameterSuffix, List<String> valueList, int maxValue) throws SQLException
    {
	for (int index = 1; index <= maxValue; index++)
	{
	    String parameter = parameterSuffix + "_" + index;
	    int arrayIndex = bindTable.get(parameter).get(0);

	    if (!bindTable.containsKey(parameter))
		throw new SQLException("Parameter '" + parameter + "' not found.");

	    if (index <= valueList.size())
		ps.setString(arrayIndex, valueList.get(index - 1));
	    else
		ps.setNull(arrayIndex, java.sql.Types.VARCHAR);
	}
    }

    /**
     * Binds a Integer value to the specified variable name
     * 
     * @param parameter
     *            The SQL variable name
     * @param value
     *            The Integer value being binded
     * @throws SQLException
     */
    public void setInt(String parameter, Integer value) throws SQLException
    {
	if (!bindTable.containsKey(parameter))
	    throw new SQLException("Parameter '" + parameter + "' not found.");

	if (value != null)
	    for (int index : bindTable.get(parameter))
		ps.setInt(index, value);
	else
	    for (int index : bindTable.get(parameter))
		ps.setNull(index, java.sql.Types.INTEGER);
    }

    /**
     * Binds a Date value to the specified variable name
     * 
     * @param parameter
     *            The SQL variable name
     * @param value
     *            The Date value being binded
     * @throws SQLException
     */
    public void setDate(String parameter, java.sql.Date value) throws SQLException
    {
	if (!bindTable.containsKey(parameter))
	    throw new SQLException("Parameter '" + parameter + "' not found.");

	if (value != null)
	    for (int index : bindTable.get(parameter))
		ps.setDate(index, value);
	else
	    for (int index : bindTable.get(parameter))
		ps.setNull(index, java.sql.Types.DATE);
    }

    /**
     * Binds a Date value to the specified variable name, using the given Calendar object. The driver uses the Calendar object to construct an SQL DATE value, which the driver then sends to the
     * database. With a Calendar object, the driver can calculate the date taking into account a custom timezone. If no Calendar object is specified, the driver uses the default timezone, which is
     * that of the virtual machine running the application.
     * 
     * @param parameter
     *            The SQL variable name
     * @param value
     *            The Date value being binded
     * @param cal
     *            The Calendar object
     * @throws SQLException
     */
    public void setDate(String parameter, java.sql.Date value, Calendar cal) throws SQLException
    {
	if (!bindTable.containsKey(parameter))
	    throw new SQLException("Parameter '" + parameter + "' not found.");

	if (value != null)
	    for (int index : bindTable.get(parameter))
		ps.setDate(index, value, cal);
	else
	    for (int index : bindTable.get(parameter))
		ps.setNull(index, java.sql.Types.DATE);
    }

    /**
     * Binds a Calendar value to the specified variable name
     * 
     * @param parameter
     *            The SQL variable name
     * @param value
     *            The Calendar object being binded
     * @throws SQLException
     */
    public void setDate(String parameter, Calendar value) throws SQLException
    {
	if (!bindTable.containsKey(parameter))
	    throw new SQLException("Parameter '" + parameter + "' not found.");

	if (value != null)
	    for (int index : bindTable.get(parameter))
		ps.setDate(index, new java.sql.Date(value.getTime().getTime()));
	else
	    for (int index : bindTable.get(parameter))
		ps.setNull(index, java.sql.Types.DATE);
    }

    /**
     * Sets the value NULL to the given variable name
     * 
     * @param parameter
     *            The variable name
     * @throws SQLException
     */
    public void setNull(String parameter) throws SQLException
    {
	if (!bindTable.containsKey(parameter))
	    throw new SQLException("Parameter '" + parameter + "' not found.");

	for (int index : bindTable.get(parameter))
	    ps.setNull(index, java.sql.Types.NULL);
    }
    
    public void setNullDate(String parameter) throws SQLException
    {
    if (!bindTable.containsKey(parameter))
        throw new SQLException("Parameter '" + parameter + "' not found.");

    for (int index : bindTable.get(parameter))
        ps.setNull(index, java.sql.Types.DATE);
    }

    /**
     * Sets the value NULL to all variables
     * 
     * @throws SQLException
     */
    public void setAllNull() throws SQLException
    {
	for (String variable : bindTable.keySet())
	    setNull(variable);
    }

    /**
     * This method reads the SQL provided on #prepareStatement methods and assign an index to each variable every time it matches the regEx.
     */
    private void buildBindTable(String sql)
    {
	Pattern pattern = Pattern.compile(REGEX_PATTERN, Pattern.MULTILINE);

	Matcher matcher = pattern.matcher(sql);

	// Starts the index to count every time it matches a SQL variable
	int index = 1;

	// Finds all the matches.
	while (matcher.find())
	{
	    // Assgin the index to the matched variable
	    addIndex(matcher.group().substring(1), index);
	    index++;
	}
    }

    void addIndex(String name, int index)
    {
	ArrayList<Integer> list = bindTable.get(name);

	if (list == null)
	{
	    list = new ArrayList<Integer>();
	    bindTable.put(name, list);
	}

	list.add(index);
    }

    public PreparedStatement prepareStatement(String sql, Connection cnn) throws SQLException
    {
	ps = cnn.prepareStatement(sql);
	buildBindTable(sql);
	return ps;
    }

    public PreparedStatement prepareStatement(String sql, Connection cnn, int autoGeneratedKeys) throws SQLException
    {
	ps = cnn.prepareStatement(sql, autoGeneratedKeys);
	buildBindTable(sql);
	return ps;
    }

    public PreparedStatement prepareStatement(String sql, Connection cnn, int[] columnIndexes) throws SQLException
    {
	ps = cnn.prepareStatement(sql, columnIndexes);
	buildBindTable(sql);
	return ps;
    }

    public PreparedStatement prepareStatement(String sql, Connection cnn, String[] columnNames) throws SQLException
    {
	ps = cnn.prepareStatement(sql, columnNames);
	buildBindTable(sql);
	return ps;
    }

    public PreparedStatement prepareStatement(String sql, Connection cnn, int resultSetType, int resultSetConcurrency) throws SQLException
    {
	ps = cnn.prepareStatement(sql, resultSetType, resultSetConcurrency);
	buildBindTable(sql);
	return ps;
    }

    public PreparedStatement prepareStatement(String sql, Connection cnn, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
	ps = cnn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	buildBindTable(sql);
	return ps;
    }

    /**
     * Binds a Double value to the specified variable name
     * 
     * @param parameter
     *            The SQL variable name
     * @param value
     *            The Double object being binded
     * @throws SQLException
     */
    public void setDouble(String parameter, Double value) throws SQLException
    {
	if (!bindTable.containsKey(parameter))
	    throw new SQLException("Parameter '" + parameter + "' not found.");

	if (value != null)
	    for (int index : bindTable.get(parameter))
		ps.setDouble(index, value);
	else
	    for (int index : bindTable.get(parameter))
		ps.setNull(index, java.sql.Types.DOUBLE);
    }

    /**
     * Binds a Long value to the specified variable name
     * 
     * @param parameter
     *            The SQL variable name
     * @param value
     *            The Double object being binded
     * @throws SQLException
     */
    public void setLong(String parameter, Long value) throws SQLException
    {
	if (!bindTable.containsKey(parameter))
	    throw new SQLException("Parameter '" + parameter + "' not found.");

	if (value != null)
	    for (int index : bindTable.get(parameter))
		ps.setLong(index, value);
	else
	    for (int index : bindTable.get(parameter))
		ps.setNull(index, java.sql.Types.DOUBLE);
    }

    /**
     * Binds a Object value to the specified variable name
     * 
     * @param parameter
     *            The SQL variable name
     * @param value
     *            The Double object being binded
     * @throws SQLException
     */
    public void setObject(String parameter, Object value) throws SQLException
    {
	if (!bindTable.containsKey(parameter))
	    throw new SQLException("Parameter '" + parameter + "' not found.");

	if (value != null)
	    for (int index : bindTable.get(parameter))
		ps.setObject(index, value);
	else
	    for (int index : bindTable.get(parameter))
		ps.setNull(index, java.sql.Types.OTHER);
    }

    public void setBigDecimal(String parameter, BigDecimal value)
	    throws SQLException
    {
	if (!bindTable.containsKey(parameter))
	    throw new SQLException("Parameter '" + parameter + "' not found.");

	if (value != null)
	    for (int index : bindTable.get(parameter))
		ps.setBigDecimal(index, value);
	else
	    for (int index : bindTable.get(parameter))
		ps.setNull(index, java.sql.Types.BIGINT);

    }

    /**
     * Binds a java.sql.Timestamp value to the specified variable name.
     * The driver converts this to an SQL TIMESTAMP value when it sends it to the database.
     * 
     * @param parameter
     *            The SQL variable name
     * @param value
     *            The Timestamp value being binded
     * @throws SQLException
     */
    public void setTimestamp(String parameter, java.sql.Timestamp value) throws SQLException
    {
	if (!bindTable.containsKey(parameter))
	    throw new SQLException("Parameter '" + parameter + "' not found.");

	if (value != null)
	    for (int index : bindTable.get(parameter))
		ps.setTimestamp(index, value);
	else
	    for (int index : bindTable.get(parameter))
		ps.setNull(index, java.sql.Types.TIMESTAMP);
    }

    /**
     * Binds a java.sql.Timestamp value, using the given Calendar object.
     * The driver uses the Calendar object to construct an SQL TIMESTAMP value, which the driver then sends to the database. With a Calendar object, the driver can calculate the timestamp taking into
     * account a custom timezone. If no Calendar object is specified, the driver uses the default timezone, which is that of the virtual machine running the application.
     * 
     * @param parameter
     *            The SQL variable name
     * @param value
     *            The Timestamp value being binded
     * @param cal
     *            The Calendar object
     * @throws SQLException
     */
    public void setTimestamp(String parameter, java.sql.Timestamp value, Calendar cal) throws SQLException
    {
	if (!bindTable.containsKey(parameter))
	    throw new SQLException("Parameter '" + parameter + "' not found.");

	if (value != null)
	    for (int index : bindTable.get(parameter))
		ps.setTimestamp(index, value, cal);
	else
	    for (int index : bindTable.get(parameter))
		ps.setNull(index, java.sql.Types.TIMESTAMP);
    }

    /**
     * Given a suffix and a size will return a String in the format; :suffix_1, :suffix_2 ... :suffix_[size]
     * 
     * @param suffix
     * @param size
     * @return
     */
    public String getVariableList(String suffix, int size)
    {
	if (size < 1)
	    return "";

	StringBuilder resultList = new StringBuilder();

	for (int i = 1; i <= size; i++)
	{
	    resultList.append(":").append(suffix).append("_").append(i).append(", ");
	}

	return resultList.substring(0, resultList.length() - 2);
    }
}
