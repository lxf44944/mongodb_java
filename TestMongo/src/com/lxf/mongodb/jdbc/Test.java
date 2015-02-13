package com.lxf.mongodb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Test {
	public static void main(String[] args) {
		try {
			Class.forName("mongodb.jdbc.MongoDriver");
			Connection c = DriverManager
					.getConnection("jdbc:mongo://localhost:27017/lxf");
			PreparedStatement st = c.prepareStatement("select * from students");
			ResultSet res = st.executeQuery();
			while (res.next()) {
				System.out.println(res.getString("id"));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
