package db.pool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;


public class Test {
	private static final String ODL_POOL_URL = "odl_pool";	
	
	public static void main(String[] args) throws Exception{		
		//this will create a connection pool with 5 max connections and 1 min connection
		new DBDriver("oracle.jdbc.driver.OracleDriver",
	            "jdbc:oracle:thin:@yourhost:port:DB", "userid","password", 5, 1, ODL_POOL_URL, "select * from dual");
		Connection connection = DriverManager.getConnection(ODL_POOL_URL);
		//now this connection can be used for all db related work
		//and connection.close() will close the return the connection to the pool.
	}
}
