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
import com.mongodb.MongoClient;

/**
 * MongoDB操作
 * 
 * @author 刘向峰
 * 
 */
public class MongoDB {

	private MongoClient mongo = null;
	private DB db = null;
	private DBCollection students = null;

	/**
	 * 初始化参数，获得集合对象
	 * 
	 * @param dbName
	 *            数据库名
	 * @param collectionName
	 *            集合名
	 */
	public MongoDB(String dbName, String collectionName) {
		super();
		try {
			mongo = new MongoClient();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		db = mongo.getDB(dbName);
		students = db.getCollection(collectionName);
	}

	/**
	 * 在对象销毁时自动关闭资源
	 */
	@Override
	protected void finalize() throws Throwable {
		// TODO Auto-generated method stub
		this.close();
		super.finalize();
	}

	/**
	 * 关闭资源
	 */
	public void close() {
		System.out.println("------------关闭资源---------------");
		// TODO Auto-generated method stub
		if (mongo != null) {
			mongo.close();
			mongo = null;
		}

		if (db != null) {
			// 结束Mongo数据库的事务请求
			db.requestDone();
			db = null;
		}
	}

	/**
	 * 删除数据
	 */
	public void delete() {
		System.out.println("------------删除---------------");
		BasicDBObject object = new BasicDBObject();
		object.put("age", 40);
		object.put("name", "小李1");
		students.remove(object);

	}

	/**
	 * 修改数据
	 */
	public void update() {
		// TODO Auto-generated method stub
		System.out.println("------------修改---------------");
		BasicDBObject doc = new BasicDBObject();
		BasicDBObject res = new BasicDBObject();
		res.put("age", 40);
		System.out.println("将数据集中的所有文档的age修改成40！");
		doc.put("$set", res);
		students.update(new BasicDBObject(), doc, false, true);
	}

	/**
	 * 插入数据
	 */
	public void insert() {
		System.out.println("------------插入---------------");
		List<DBObject> dbList = new ArrayList<DBObject>();
		BasicDBObject stu1 = null;

		for (int i = 0; i < 10; i++) {
			stu1 = new BasicDBObject();
			stu1.put("name", "小李" + (i + 1));
			stu1.put("age", 30 + i);
			stu1.put("id", 1 + i);
			dbList.add(stu1);
		}

		students.insert(dbList);
	}

	/**
	 * 打印数据
	 */
	public void printValues() {
		// System.out.println("------------打印---------------");
		DBCursor cur = students.find();
		System.out.println(cur.count() + "条记录");
		while (cur.hasNext()) {
			BasicDBObject bdbObj = (BasicDBObject) cur.next();
			if (bdbObj != null) {
				System.out.print("id:" + bdbObj.getString("id") + "\t");
				System.out.print("name:" + bdbObj.getString("name") + "\t");
				System.out.print("age:" + bdbObj.getInt("age") + "\n");
			}
		}
	}
}
