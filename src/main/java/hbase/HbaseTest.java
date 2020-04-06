package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest {
	public static final String ArticleTableName = "article_action_table";
	public static final String ArticleColumnFamily = "cf";
	/**
	 * 配置
	 */
	static Configuration config = null;
	static {
		config = HBaseConfiguration.create();//配置
		config.set("hbase.zookeeper.quorum", "47.97.167.185");//zookeeper地址
		config.set("hbase.zookeeper.property.clientPort", "2181");//zookeeper端口
	}
	/**
	 * 创建一个表
	 * @param tableName：表名
	 * @param familys：列族
	 */
	public void createTable(String tableName, String[] familys) {
		try {
			HBaseAdmin admin = new HBaseAdmin(config);//hbase表管理
			if (admin.tableExists(tableName)) {//表是否存在
				System.out.println(tableName + "表已经存在!");
			} else {
				HTableDescriptor desc = new HTableDescriptor(tableName);//表的schema
				for (int i = 0; i < familys.length; i++) {
					HColumnDescriptor family = new HColumnDescriptor(familys[i]);//设计列族
					desc.addFamily(family);
				}
				admin.createTable(desc);//创建表
				System.out.println("创建表 \'" + tableName + "\' 成功!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 删除表
	 * @param tableName
	 */
	public void deleteTable(String tableName) {
		try {
			HBaseAdmin admin = new HBaseAdmin(config);
			if (!admin.tableExists(tableName)) {//表不存在
				System.out.println(tableName + " is not exists!");
			} else {
				admin.disableTable(tableName);//废弃表
				admin.deleteTable(tableName);//删除表
				System.out.println(tableName + " is delete!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 插入数据
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 */
	public void insertData(String tableName, String rowKey, String family,
			String qualifier, String value) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 10);//创建池
			table = pool.getTable(tableName);//获取表
			Put put = new Put(Bytes.toBytes(rowKey));//获取put，用于插入
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),Bytes.toBytes(value));//封装信息
			table.put(put);//添加记录
			/*
			 *批量插入
				List<Put> list = new ArrayList<Put>();
				Put put = new Put(Bytes.toBytes(rowKey));//获取put，用于插入
				put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),Bytes.toBytes(value));//封装信息
				list.add(put);
				table.put(list);//添加记录
			 * */
			System.out.println("insert a data successful!");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();//关闭表
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}


	/**
	 * 插入数据
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 */
	public void insertData1(String tableName, String rowKey, String family,
						   String qualifier, Long value) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 10);//创建池
			table = pool.getTable(tableName);//获取表
			table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes(ArticleColumnFamily), Bytes.toBytes(qualifier), 1L);
			/*
			 *批量插入
				List<Put> list = new ArrayList<Put>();
				Put put = new Put(Bytes.toBytes(rowKey));//获取put，用于插入
				put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),Bytes.toBytes(value));//封装信息
				list.add(put);
				table.put(list);//添加记录
			 * */
			System.out.println("insert a data successful!");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();//关闭表
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 删除数据
	 * @param tableName
	 * @param rowKey
	 */
	public void deleteData(String tableName, String rowKey) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 10);
			table = pool.getTable(tableName);
			Delete del = new Delete(Bytes.toBytes(rowKey));//创建delete
			table.delete(del);//删除
			System.out.println("delete a data successful");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 通过rewKey查询
	 * @param tableName
	 * @param rowKey
	 */
	public void queryByRowKey(String tableName, String rowKey) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 10);
			table = pool.getTable(tableName);
			Get get = new Get(rowKey.getBytes());//创建get
			Result row = table.get(get);//获取航记录
			//row.getValue(family, qualifier);//分别获取cell信息
			for (KeyValue kv : row.raw()) {//循环列信息
				System.out.print(new String(kv.getRow()) + " ");
				System.out.print(new String(kv.getFamily()) + ":");
				System.out.print(new String(kv.getQualifier()) + " = ");
				System.out.print(new String(kv.getValue()));
				System.out.print(" timestamp = " + kv.getTimestamp() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 查询表的所有数据
	 * 即scan tablename
	 * @param tableName
	 */
	public void queryAll(String tableName) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 10);
			table = pool.getTable(tableName);
			Scan scan = new Scan();//创建scan
			scan.setStartRow("1".getBytes());//添加开始rowkey
			scan.setStopRow("4322".getBytes());//结束rowkey
			scan.addColumn("info".getBytes(), "name".getBytes());
			ResultScanner scanner = table.getScanner(scan);//查询，返回结果
			for (Result row : scanner) {//循环scan，得到每行记录
				//row.getValue(family, qualifier);//分别获取cell信息
				System.out.println("\nRowkey: " + new String(row.getRow()));
				for (KeyValue kv : row.raw()) {//循环每列数据
					System.out.print(new String(kv.getRow()) + " ");
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " = ");
					System.out.print(new String(kv.getValue()));
					System.out.print(" timestamp = " + kv.getTimestamp() + "\n");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 查询表的所有数据
	 * 即scan tablename
	 * @param tableName
	 */
	public void queryAllArticle(String tableName) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 10);
			table = pool.getTable(tableName);
			Scan scan = new Scan();//创建scan
			scan.setStartRow("1".getBytes());//添加开始rowkey
			scan.setStopRow("4322".getBytes());//结束rowkey
			scan.addColumn("cf".getBytes(), "add".getBytes());
			scan.addColumn("cf".getBytes(), "del".getBytes());
			ResultScanner scanner = table.getScanner(scan);//查询，返回结果
			for (Result row : scanner) {//循环scan，得到每行记录
				//row.getValue(family, qualifier);//分别获取cell信息
				System.out.println("\nRowkey: " + new String(row.getRow()));
				for (KeyValue kv : row.raw()) {//循环每列数据
					System.out.print(new String(kv.getRow()) + " ");
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " = ");
					System.out.print(new String(kv.getValue()));
					System.out.print(" timestamp = " + kv.getTimestamp() + "\n");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 查询表的所有数据
	 * 即scan tablename
	 * @param tableName
	 */
	public void queryAllArticle1(String tableName,String rowKey) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 10);
			table = pool.getTable(tableName);

//			Increment incr1 = new Increment(Bytes.toBytes(rowKey));
//			incr1.addColumn(Bytes.toBytes(ArticleColumnFamily), Bytes.toBytes("query"),10);
//			incr1.addColumn(Bytes.toBytes(ArticleColumnFamily), Bytes.toBytes("tenant_id"), 2);

//			Result result = table.increment(incr1);
			table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes(ArticleColumnFamily), Bytes.toBytes("query"), 1L); // pv +10
			Get g = new Get(Bytes.toBytes(rowKey));
//			g.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("pv"));
			Result row = table.get(g);
			System.out.println("\nRowkey: " + new String(row.getRow()));
			System.out.println(Bytes.toLong(row.getValue(Bytes.toBytes("cf"), Bytes.toBytes("query"))));
//			byte[] value = row.getValue("cf".getBytes(), "pv2".getBytes());//分别获取cell信息
//			System.out.print(new String(value) + " ");
			for (KeyValue kv : row.raw()) {//循环每列数据
				System.out.print(new String(kv.getRow()) + " ");
				System.out.print(new String(kv.getFamily()) + ":");
				System.out.print(new String(kv.getQualifier()) + " = ");
				System.out.print(new String(kv.getValue()));
				System.out.print(" timestamp = " + kv.getTimestamp() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 查询表的所有数据
	 * 即scan tablename
	 * @param tableName
	 */
	public void incrementColumnValue(String tableName,String rowKey) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 10);
			table = pool.getTable(tableName);
			Get g = new Get(rowKey.getBytes());
			Result row = table.get(g);
			System.out.println("\nRowkey: " + new String(row.getRow()));
			byte[] value = row.getValue("cf".getBytes(), "query".getBytes());//分别获取cell信息
			System.out.print(new String(value) + " ");
//			for (KeyValue kv : row.raw()) {//循环每列数据
//				System.out.print(new String(kv.getRow()) + " ");
//				System.out.print(new String(kv.getFamily()) + ":");
//				System.out.print(new String(kv.getQualifier()) + " = ");
//				System.out.print(new String(kv.getValue()));
//				System.out.print(" timestamp = " + kv.getTimestamp() + "\n");
//			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 过滤器查询
	 * @param tableName
	 * @param arr
	 */
	public static void selectByFilter(String tableName, List<String> arr) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 10);
			table = pool.getTable(tableName);
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);// 各个条件之间是“或”的关系，默认是“与”
			Scan s1 = new Scan();
//			for (String v : arr) {
//				String[] s = v.split(",");
//				filterList.addFilter(new SingleColumnValueFilter(Bytes
//						.toBytes(s[0]), Bytes.toBytes(s[1]), CompareOp.EQUAL,
//						Bytes.toBytes(s[2])));
//				// 添加下面这一行后，则只返回指定的cell，同一行中的其他cell不返回
//				// s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
//			}

//			Filter filter = new RowFilter(CompareOp.EQUAL,new RegexStringComparator(regex));

			// SingleColumnValueFilter 用于测试列值相等 (CompareOp.EQUAL ), 不等
			// (CompareOp.NOT_EQUAL),或范围 (e.g., CompareOp.GREATER).
			// 下面示例检查列值和字符串'values' 相等...
			// SingleColumnValueFilter f = new
			// SingleColumnValueFilter(Bytes.toBytes("cFamily"),
			// Bytes.toBytes("column"), CompareFilter.CompareOp.EQUAL,
			// Bytes.toBytes("values"));
			// SingleColumnValueFilter f = new
			// SingleColumnValueFilter(Bytes.toBytes("cFamily"),
			// Bytes.toBytes("column"), CompareFilter.CompareOp.EQUAL,new
			// SubstringComparator("values"));
			// s1.setFilter(f);
			// ColumnPrefixFilter 用于指定列名前缀值相等
			// ColumnPrefixFilter f = new
			// ColumnPrefixFilter(Bytes.toBytes("values"));
			// s1.setFilter(f);
			// MultipleColumnPrefixFilter 和 ColumnPrefixFilter 行为差不多，但可以指定多个前缀
			// byte[][] prefixes = new byte[][] {Bytes.toBytes("value1"),
			// Bytes.toBytes("value2")};
			// Filter f = new MultipleColumnPrefixFilter(prefixes);
			// s1.setFilter(f);
			// QualifierFilter 是基于列名的过滤器。
			// Filter f = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new
			// BinaryComparator(Bytes.toBytes("col5")));
			// s1.setFilter(f);
			// RowFilter 是rowkey过滤器,通常根据rowkey来指定范围时，使用scan扫描器的StartRow和StopRow
			// 方法比较好。Rowkey也可以使用。
			 Filter f = new
			 RowFilter(CompareOp.EQUAL, new
			 RegexStringComparator(".*2019-03-20$"));//正则获取结尾为5的行
			filterList.addFilter(f);
			 Filter f1 = new
			 RowFilter(CompareOp.EQUAL, new
			 RegexStringComparator(".*2019-03-21$"));//正则获取结尾为5的行
			filterList.addFilter(f);
			filterList.addFilter(f1);
			s1.setFilter(filterList);
			 s1.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("add"));
			 s1.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("query"));
			ResultScanner rs = table.getScanner(s1);
			for (Result rr = rs.next(); rr != null; rr = rs
					.next()) {
				for (KeyValue kv : rr.list()) {
					System.out.println("row : " + new String(kv.getRow()));
					System.out.println("column : " + new String(kv.getFamily())
							+ ":" + new String(kv.getQualifier()));
					System.out.println("value : " + new String(kv.getValue()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	

	public static void main(String[] args) {
		HbaseTest ht = new HbaseTest();
//		ht.createTable("htest", new String[] { "fcol1", "fcol2" });
//		ht.createTableSplit("htest", new String[]{"fcol1","fcol2"});
//		ht.createTableSplit2("htest", new String[]{"fcol1","fcol2"});
//		ht.deleteTable("htest");
//		for (int i = 0; i < 1000; i++) {
//			ht.insertData("htest", "a"+i, "fcol1", "c1", "aaa"+i);
//			ht.insertData("htest", "a"+i, "fcol1", "c2", "bbb"+i);
//		}
//		ht.deleteData("htest", "a1");
//		ht.queryByRowKey("htest", "a2");
//		ht.queryAll("htest");
//		List list=new ArrayList();
//		list.add("fcol1,c1,aaa1");
//		list.add("fcol1,c1,aaa2");
//		list.add("fcol1,c1,aaa3");
//		ht.selectByFilter("htest",list );
		List list=new ArrayList();
		list.add("cf,add,N");
//		list.add("fcol1,c1,aaa2");
//		list.add("fcol1,c1,aaa3");
		ht.selectByFilter(ArticleTableName,list );
	}

}
