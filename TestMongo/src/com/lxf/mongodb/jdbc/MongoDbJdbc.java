package com.lxf.mongodb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class MongoDbJdbc {
	private static Connection con;
	private static Statement stmt;
	private static ResultSet rst;

	public static void main(String[] args) {
		try {
			// 1、Create a new instance of the JDBC Driver for MongoDB and make a
			// connection.
			Class.forName("mongodb.jdbc.MongoDriver");
			// 2、Connect to the URL. The last part is the database name (tpch in
			// this case).
			String url = "jdbc:mongo://localhost:27017/lxf";
			con = DriverManager.getConnection(url, "dbuser", "dbuser");
			// 3、Create a statement and submit a query. See the features and SQL
			// examples supported by the JDBC driver for MongoDB.
			stmt = con.createStatement();
			String sql = "SELECT * FROM students WHERE id >= 10;";
			rst = stmt.executeQuery(sql);
			// 4、Print out your results.
			ResultSetMetaData meta = rst.getMetaData();
			int numColumns = meta.getColumnCount();
			System.out.print(meta.getColumnName(1));
			for (int j = 2; j <= meta.getColumnCount(); j++)
				System.out.print(", " + meta.getColumnName(j));
			System.out.println();
			while (rst.next()) {
				System.out.print(rst.getObject(1));
				for (int j = 2; j <= numColumns; j++)
					System.out.print(", " + rst.getObject(j));
				System.out.println();
			}
			// 5、Close the statement and connection.
			rst.close();
			stmt.close();
			con.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
