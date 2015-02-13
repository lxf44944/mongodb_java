package com.lxf.mongodb.mongo;

/**
 * 测试MongoDB类
 * @author 刘向峰
 *
 */
public class Main {

	public static void main(String[] args) {
		//数据库为lxf，集合为students
		MongoDB mongoDB = new MongoDB("lxf", "students");
		mongoDB.printValues();
		mongoDB.insert();
		mongoDB.printValues();
		mongoDB.update();
		mongoDB.printValues();
		mongoDB.delete();
		mongoDB.printValues();
		mongoDB.close();
	}
}
