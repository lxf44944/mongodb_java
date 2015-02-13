package com.lxf.mongodb.jdbc;

/*
 * This code was created by Unity Data Inc. (www.unityjdbc.com).
 * 
 * It may be freely used, 

 modified, and distributed with no restrictions.
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Demonstrates how to write queries using the MongoJDBC driver. Need to have
 * 
 * write privileges on database in order to update _schema as this will be built
 * by
 * 
 * Unity. If you do not have write privileges, schema can be stored locally.
 * 
 * Force rebuild of schema on
 * 
 * connection jdbc:mongo://url:port/dbName?rebuildschema=true
 * 
 * Force rebuild of schema and store locally
 * 
 * 
 * jdbc:mongo://url:port/dbName?rebuildschema=true&schema=tmp.txt
 */
public class ExampleMongoJDBC {
	/**
	 * 
	 Main method.
	 * 
	 * @param args
	 *            no arguments required
	 */
	@SuppressWarnings({ "nls" })
	public static void main(String[] args) {
		Connection con = null;
		String sql;

		try {
			// Create new instance of Mongo JDBC Driver and make connection.

			System.out.println("Registering Driver.");

			// Note: Registering driver using Class.forName() is not required.
			Class.forName("mongodb.jdbc.MongoDriver");

			// Connect to the URL. The last part is the database name (tpch in
			// this case).
			// Note: You do not have permissions to write to this database or
			// rebuild the schema.
			String url = "jdbc:mongo://localhost:27017/lxf?rebuildschema=false";

			System.out.println("Making a connection to: " + url);
			con = DriverManager.getConnection(url, "dbuser", "dbuser");
			System.out.println("Connection successful.\n");

			// Create a statement and submit a query.
			/*
			 * Sample queries
			 */

			// SELECT with a WHERE filter
			sql = "SELECT * FROM nation WHERE n_name >= 'C';";

			doQuery(con, sql);

			// SELECT all entries
			sql = "SELECT * FROM nation;";

			doQuery(con, sql);

			// SELECT only some fields (columns)
			sql = "SELECT n_nationkey, n_name FROM nation";
			doQuery(con, sql);

			// SELECT fields with field renaming
			sql =

			"SELECT n_nationkey AS nkey, n_name name FROM nation";
			doQuery(con, sql);

			// SELECT fields with expressions
			sql = "SELECT n_nationkey + 1 AS NewKey, 'name:'+ n_name AS name FROM nation";

			doQuery(con, sql);

			// SELECT DISTINCT
			sql = "SELECT DISTINCT n_regionkey FROM nation";

			doQuery(con, sql);

			// JOIN between two collections
			sql = "SELECT * FROM nation n, region r WHERE n.n_regionkey = r.r_regionkey";
			doQuery(con, sql);

			// INNER JOIN between two collections
			sql = "SELECT * FROM nation n INNER JOIN region r ON n.n_regionkey = r.r_regionkey";

			doQuery(con, sql);

			// LEFT OUTER JOIN between two collections
			sql = "SELECT * FROM nation n LEFT OUTER JOIN region r ON n.n_regionkey =  r.r_regionkey";
			doQuery(con, sql);

			// RIGHT OUTER JOIN between two collections
			sql = "SELECT * FROM nation n RIGHT OUTER JOIN region r ON n.n_regionkey = r.r_regionkey";
			doQuery(con, sql);

			// FULL OUTER JOIN between two collections
			sql = "SELECT * FROM nation n FULL OUTER JOIN region r ON n.n_regionkey = r.r_regionkey";

			doQuery(con, sql);

			// Multiple filters
			sql = "SELECT * FROM nation WHERE n_name <= 'UNITED STATES' AND n_nationkey > 3";
			doQuery(con, sql);

			// LIKE filter
			sql = "SELECT * FROM nation WHERE n_name LIKE '%AN%'";
			doQuery(con, sql);

			// COUNT rows in a collection
			sql = "SELECT COUNT(*) FROM nation";
			doQuery(con, sql);

			// COUNT with a GROUP BY
			sql = "SELECT n_regionkey, COUNT(*) as cnt FROM nation GROUP BY n_regionkey";

			doQuery(con, sql);

			// Aggregate functions
			sql = "SELECT n_regionkey, COUNT(n_nationkey) as cnt, MAX(n_nationkey) as max, MIN(n_nationkey) as min, AVG(n_nationkey) as avg, SUM(n_nationkey) as sum FROM nation GROUP BY n_regionkey";
			doQuery(con, sql);

			// ORDER BY sorting
			sql = "SELECT * FROM nation ORDER BY n_name ASC";
			doQuery(con, sql);

			// ORDER BY expressions

			sql = "SELECT * FROM nation ORDER BY n_nationkey+5 ASC";
			doQuery(con, sql);

			// LIMIT

			sql = "SELECT * FROM nation LIMIT 5";
			doQuery(con, sql);

			// LIMIT with OFFSET

			sql = "SELECT * FROM nation LIMIT 5 OFFSET 2";
			doQuery(con, sql);

			// UNION (from two or more collections)
			sql = "SELECT r_regionkey FROM region UNION SELECT n_regionkey FROM nation";

			doQuery(con, sql);

			// SELECT DISTINCT -Single value
			sql = "SELECT DISTINCT r_regionkey FROM region";
			doQuery(con, sql);

			// SELECT DISTINCT - single field
			sql = "SELECT DISTINCT n_regionkey FROM nation";
			doQuery(con, sql);

			// SELECT DISTINCT - multiple fields (promotes to Unity)
			sql = "SELECT DISTINCT r_regionkey, r_name FROM region";
			doQuery(con, sql);

			// Complex select will promote to Unity
			sql = "SELECT c_name as name, SUM(o_totalprice) as total FROM customer, orders WHERE c_custkey = o_custkey GROUP by c_name LIMIT 10";
			doQuery(con, sql);

			// Selection with predicate will run natively through driver
			sql = "SELECT r_name FROM region WHERE r_name LIKE 'A%'";
			doQuery(con, sql);

			sql = "SELECT r_name FROM region WHERE r_name LIKE 'A%' AND r_name LIKE '%CA'";
			doQuery(con, sql);

			sql = "SELECT r_name FROM region WHERE r_name NOT LIKE 'A%' AND r_name LIKE '%CA'";
			doQuery(con, sql);

			sql = "SELECT r_name FROM region WHERE r_name LIKE 'A%' OR r_name LIKE '%CA'";
			doQuery(con, sql);

			// Complex select will promote to Unity
			sql = "SELECT c_name as name, SUM(o_totalprice) as total FROM customer, orders WHERE c_custkey = o_custkey AND c_name LIKE '%6' GROUP by c_name HAVING SUM(o_totalprice) > 1000000.0 LIMIT 100";
			doQuery(con, sql);
		} catch (Exception ex) {

			System.out.println("Exception: " + ex);
			ex.printStackTrace();
		} finally {

			if (con != null) {
				try {
					// Close the connection
					con.close();
				} catch (SQLException ex)

				{
					System.out.println("SQLException: " + ex);
				}
			}
		}
	}

	/**
	 * Helper method to run SQL query on MongoDB.
	 * 
	 * @param con
	 *            Connection object to MongoDB
	 * 
	 * 
	 * @param sql
	 *            SQL statement to run
	 * 
	 * @throws SQLException
	 *             if a database error occurs
	 */
	public static void doQuery(Connection con, String sql) throws SQLException {
		Statement stmt =

		con.createStatement();

		System.out.println("\n\nExecuting query:      " + sql);
		ResultSet rst = stmt.executeQuery(sql);

		System.out.println("Executed Mongo query: ");
		System.out.println("Query execution complete.\n");

		System.out.println

		("Results:\n");

		int count = printResults(rst);

		// Close statement
		rst.close();

		stmt.close();

		System.out.println("\nSUCCESS.  Total results: " + count);
	}

	/**
	 * Helper
	 * 
	 * method to print out the resultSet.
	 * 
	 * @param rst
	 *            the resultSet returned from the query execution.
	 * 
	 @return the number of tuples returned
	 * @throws SQLException
	 *             if a database error occurs
	 */
	public static int printResults(ResultSet rst) throws SQLException {
		// Print out your results

		ResultSetMetaData meta = rst.getMetaData();
		int numColumns = meta.getColumnCount();

		System.out.print(meta.getColumnName(1));
		for (int j = 2; j <= meta.getColumnCount(); j++)

			System.out.print(", " + meta.getColumnName(j));
		System.out.println();

		int count = 0;
		while

		(rst.next()) {
			System.out.print(rst.getObject(1));
			for (int j = 2; j <= numColumns;

			j++)
				System.out.print(", " + rst.getObject(j));
			System.out.println();

			count++;
		}
		return count;
	}
}