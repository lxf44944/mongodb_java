package com.lxf.mongodb.mongo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class ExampleMongoDB {

	/**
	 * java mongodb的数据插入、读取、更新、删除
	 */

	private static Mongo m = null;
	private static DB db = null;

	// 数据集合名称
	private static final String COLLECTION_NAME = "mcpang";

	/*
	 * 测试java处理mongodb的增、删、改、查操作
	 */
	public static void main(String[] args) {
		// 获取数据库连接
		startMongoDBConn();
		// 保存数据
		createColData();
		// 读取数据
		readColData();
		// 更新数据
		updateColData();
		// 读取数据
		readColData();
		// 删除数据
		deleteColData();
		// 读取数据
		readColData();
		// 删除数据集
		db.getCollection(COLLECTION_NAME).drop();
		// 关闭数据库连接
		stopMondoDBConn();

	}

	/**
	 * 数据插入 测试数据： 【name:小李、age:30、address:北京】 【name:小张、age:25、address:天津】
	 * 
	 * @return
	 */
	private static void createColData() {
		DBCollection dbCol = db.getCollection(COLLECTION_NAME);
		System.out.println("向数据集中插入数据开始：");
		List<DBObject> dbList = new ArrayList<DBObject>();
		BasicDBObject doc1 = new BasicDBObject();
		doc1.put("name", "小李");
		doc1.put("age", 30);
		doc1.put("address", "北京");
		dbList.add(doc1);

		BasicDBObject doc2 = new BasicDBObject();
		doc2.put("name", "小张");
		doc2.put("age", 25);
		doc2.put("address", "天津");
		dbList.add(doc2);

		dbCol.insert(dbList);
		System.out.println("向数据集中插入数据完成！");
		System.out.println("------------------------------");
	}

	/**
	 * 数据读取
	 */
	private static void readColData() {
		DBCollection dbCol = db.getCollection(COLLECTION_NAME);
		DBCursor ret = dbCol.find();
		System.out.println("从数据集中读取数据：");
		while (ret.hasNext()) {
			BasicDBObject bdbObj = (BasicDBObject) ret.next();
			if (bdbObj != null) {
				System.out.println("name:" + bdbObj.getString("name"));
				System.out.println("age:" + bdbObj.getInt("age"));
				System.out.println("address:" + bdbObj.getString("address"));
			}
		}
	}

	/**
	 * 数据更新 update(q, o, upsert, multi) update(q, o, upsert, multi, concern)
	 * update(arg0, arg1, arg2, arg3, arg4, arg5) updateMulti(q, o)
	 */
	private static void updateColData() {
		System.out.println("------------------------------");
		DBCollection dbCol = db.getCollection(COLLECTION_NAME);
		DBCursor ret = dbCol.find();
		BasicDBObject doc = new BasicDBObject();
		BasicDBObject res = new BasicDBObject();
		res.put("age", 40);
		System.out.println("将数据集中的所有文档的age修改成40！");
		doc.put("$set", res);
		dbCol.update(new BasicDBObject(), doc, false, true);
		System.out.println("更新数据完成！");
		System.out.println("------------------------------");
	}

	/**
	 * 数据删除
	 */
	private static void deleteColData() {
		System.out.println("------------------------------");
		DBCollection dbCol = db.getCollection(COLLECTION_NAME);
		System.out.println("删除【小李】！");
		BasicDBObject doc = new BasicDBObject();
		doc.put("name", "小李");
		dbCol.remove(doc);
		System.out.println("------------------------------");
	}

	/**
	 * 关闭mongodb数据库连接
	 */
	private static void stopMondoDBConn() {
		if (null != m) {
			if (null != db) {
				// 结束Mongo数据库的事务请求
				try {
					db.requestDone();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			try {
				m.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			m = null;
			db = null;
		}
	}

	/**
	 * 获取mongodb数据库连接
	 */
	private static void startMongoDBConn() {
		try {
			// Mongo(p1, p2):p1=>IP地址 p2=>端口
			m = new Mongo("127.0.0.1", 27017);
			// 根据mongodb数据库的名称获取mongodb对象
			db = m.getDB("yyl");
			// 校验用户密码是否正确
			if (!db.authenticate("yyl", "yyl123".toCharArray())) {
				System.out.println("连接MongoDB数据库,校验失败！");
			} else {
				System.out.println("连接MongoDB数据库,校验成功！");
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
}
