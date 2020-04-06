package hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import utils.RandomDate;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class OneHbase {
	public static final String TableName = "user_action_table";
	public static final String ColumnFamily = "info";
	public static final String ColumnFamily2 = "info2";
	public static final String ArticleTableName = "article_action_table";
	public static final String ArticleTablePre = "article_";
	public static final String ArticleColumnFamily = "cf";

	@Test
	public void test() {
		System.out.println(1111);
		 HbaseTest hbaseTest = new HbaseTest();
		 /*String[] familys=new  String[]{ColumnFamily,ColumnFamily2};
		 hbaseTest.createTable(TableName, familys);*/
		 hbaseTest.insertData(TableName, "43211", ColumnFamily, "name", "lisi1");
		 hbaseTest.insertData(TableName, "43211", ColumnFamily, "age", "181");
		 hbaseTest.insertData(TableName, "43211", ColumnFamily, "password", "322424");
		 
		 hbaseTest.insertData(TableName, "43212", ColumnFamily, "name", "lisi1");
		 hbaseTest.insertData(TableName, "43212", ColumnFamily, "age", "182");
		 hbaseTest.insertData(TableName, "43212", ColumnFamily, "password", "322424");
		 
		 hbaseTest.insertData(TableName, "43213", ColumnFamily, "name", "lisi1");
		 hbaseTest.insertData(TableName, "43213", ColumnFamily, "age", "183");
		 hbaseTest.insertData(TableName, "43213", ColumnFamily2, "Emile", "2513120790@qq.com");
		 
		 hbaseTest.insertData(TableName, "43214", ColumnFamily, "name", "lisi1");
		 hbaseTest.insertData(TableName, "43214", ColumnFamily, "password", "322424");
		 hbaseTest.insertData(TableName, "43214", ColumnFamily2, "Emile", "2513120790@qq.com");
		 
		 hbaseTest.insertData(TableName, "43215", ColumnFamily, "name", "lisi1");
		 hbaseTest.insertData(TableName, "43215", ColumnFamily, "age", "185");
		 hbaseTest.insertData(TableName, "43215", ColumnFamily, "password", "322424");
		 hbaseTest.insertData(TableName, "43215", ColumnFamily2, "Emile", "2513120790@qq.com");
		 
		 hbaseTest.insertData(TableName, "43216", ColumnFamily, "age", "186");
		 hbaseTest.insertData(TableName, "43216", ColumnFamily, "password", "322424");
		 hbaseTest.insertData(TableName, "43216", ColumnFamily2, "Emile", "2513120790@qq.com");
		 
		 hbaseTest.insertData(TableName, "43217", ColumnFamily, "name", "lisi1");
		 hbaseTest.insertData(TableName, "43217", ColumnFamily, "age", "187");
		 hbaseTest.insertData(TableName, "43217", ColumnFamily, "password", "322424");
		 hbaseTest.insertData(TableName, "43217", ColumnFamily2, "Emile", "2513120790@qq.com");
		 
		 hbaseTest.insertData(TableName, "43218", ColumnFamily, "name", "lisi1");
		 hbaseTest.insertData(TableName, "43218", ColumnFamily, "age", "188");
		 hbaseTest.insertData(TableName, "43218", ColumnFamily, "password", "322424");
		 hbaseTest.insertData(TableName, "43218", ColumnFamily2, "Emile", "2513120790@qq.com");
	}
	@Test
	public void creattable() {
		 HbaseTest hbaseTest = new HbaseTest();
		 String[] familys=new  String[]{ArticleColumnFamily};
		 hbaseTest.createTable(ArticleTableName, familys);
	}
	@Test
	public void insertData() {
		 HbaseTest hbaseTest = new HbaseTest();
		 hbaseTest.insertData(TableName, "abcd", ColumnFamily, "name", "李四");
		 hbaseTest.insertData(TableName, "abcd", ColumnFamily, "age", "19");
		 hbaseTest.insertData(TableName, "abcd", ColumnFamily, "password", "632574");
		 hbaseTest.insertData(TableName, "abcd", ColumnFamily2, "Emile", "1457300208@qq.com");
	}
	@Test
	public void insertData1() {
		for (int i=0;i<300;i++){
			long l = RandomDate.randomLong(1, 0);
			System.out.println(l);
		}
		String flag =RandomDate.randomLong(1,0)>0 ? "Y":"N";

	}
	@Test
	public void insertArticleData() {
		 HbaseTest hbaseTest = new HbaseTest();
		for (int i=0;i<1000;i++){
			Date date = RandomDate.randomDate("2019-01-01","2019-02-31");
			String flag =RandomDate.randomLong(1,0)>0 ? "Y":"N";
			String rowKey=ArticleTablePre+RandomDate.randomLong(2000,1000)+"_"+new SimpleDateFormat("yyyy-MM-dd").format(date);
			System.out.println(rowKey);
			hbaseTest.insertData(ArticleTableName, rowKey, ArticleColumnFamily, "add", RandomDate.randomLong(1,0)>0 ? "Y":"N");
			hbaseTest.insertData(ArticleTableName, rowKey, ArticleColumnFamily, "del", RandomDate.randomLong(1,0)>0 ? "Y":"N");
			hbaseTest.insertData(ArticleTableName, rowKey, ArticleColumnFamily, "update", RandomDate.randomLong(1,0)>0 ? "Y":"N");
			hbaseTest.insertData(ArticleTableName, rowKey, ArticleColumnFamily, "tenant_id", RandomDate.randomLong(100, 1)+"");
			hbaseTest.insertData(ArticleTableName, rowKey, ArticleColumnFamily, "library_id", RandomDate.randomLong(50000, 1000)+"");
			hbaseTest.insertData1(ArticleTableName, rowKey, ArticleColumnFamily, "query", RandomDate.randomLong(50000, 1000));

		}
//		 hbaseTest.insertData(TableName, "abcd", ColumnFamily, "name", "李四");
//		 hbaseTest.insertData(TableName, "abcd", ColumnFamily, "age", "19");
//		 hbaseTest.insertData(TableName, "abcd", ColumnFamily, "password", "632574");
//		 hbaseTest.insertData(TableName, "abcd", ColumnFamily2, "Emile", "1457300208@qq.com");
	}
	@Test
	public void queryData() {
		 HbaseTest hbaseTest = new HbaseTest();
		// hbaseTest.queryByRowKey(TableName, "abcd");
		 hbaseTest.queryAllArticle(ArticleTableName);

	}
	@Test
	public void queryData1() {
		HbaseTest hbaseTest = new HbaseTest();
		// hbaseTest.queryByRowKey(TableName, "abcd");

		//incr 'article_action_table','article_62591_2019-02-26','cf:query',1
//		hbaseTest.insertData1(ArticleTableName, "article_62591_2019-02-26", ArticleColumnFamily, "pv", 300L);
		hbaseTest.queryAllArticle1(ArticleTableName,"article_96856_2019-05-18");
	}


	//过滤器练习
	@Test
	public void rowFilterTest(){



		String[] qualifiers = new String[]{"add","library_id"};

		//row 行过滤器
		//rowFilter("FileTable","fileInfo",qualifiers,"rowkey3",CompareOperator.LESS_OR_EQUAL);

		//正则过滤器

		//正则表达式，匹配以0结尾的
		String regex  = ".*2019-01-04$";

//		rowFilterRegex(ArticleTableName,ArticleColumnFamily,qualifiers,regex, CompareFilter.CompareOp.EQUAL);

	}

//	public static Configuration conf = HBaseConfiguration.create();
//	public static Connection connection = null;
//	public static Table table = null;
//	//基于行键过滤器
//	public void rowFilterRegex(String tableName, String cfName, String[] qualifiers, String regex, CompareFilter.CompareOp operator){
//
//		try {
//			conf.set("hbase.zookeeper.quorum", "47.97.167.185");//zookeeper地址
//			conf.set("hbase.zookeeper.property.clientPort", "2181");//zookeeper端口
//			connection = ConnectionFactory.createConnection(conf);
//
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		try {
//			table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(ArticleTableName));
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
//		try(Table table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName))){
//			Scan scan = new Scan();
//
//			//为了输出特定列族特定列
//			for(String name:qualifiers){
//				scan.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(name));
//			}
//
//			Filter filter = new RowFilter(operator,new RegexStringComparator(regex));
//
//			scan.setCaching(1000);
//
//			scan.setFilter(filter);
//
//			ResultScanner scanner = table.getScanner(scan);
//
//			//输出打印
//			resultlog(scanner);
//
//			scanner.close();
//
//
//
//		}catch (IOException e){
//			e.printStackTrace();
//		}
//
//
//
//	}
//
//
//	//打印过滤
//	public void resultlog(ResultScanner scanner){
//		scanner.forEach(result -> {
//			if (result != null){
//				System.out.println("rowkey="+Bytes.toString(result.getRow()));
//				System.out.println("add="+Bytes.toString(result.getValue(Bytes.toBytes(ArticleColumnFamily),Bytes.toBytes("add"))));
//				System.out.println("library_id="+Bytes.toString(result.getValue(Bytes.toBytes(ArticleColumnFamily),Bytes.toBytes("library_id"))));
//			}
//
//		});
//	}

}
