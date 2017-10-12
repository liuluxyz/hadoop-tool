package test.testhbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * liulu5
 * 2015-10-28
 */
/**
 * @Description: TODO
 * @author liulu5
 * @date 2015-10-28 下午4:52:40
 */
public class TestHbase {

	public static Configuration configuration;
	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2581");
		configuration.set("hbase.zookeeper.quorum", "10.1.253.178,10.1.253.182,10.1.253.186");
		configuration.set("hbase.master", "10.1.253.178:60000");
	}

	public static void main(String[] args) {
//		 createTable("liulu");
//		 insertData("liulu");
//		 QueryAll("liulu");
//		 QueryByCondition1("liulu");
//		 QueryByCondition2("liulu");
//		 QueryByCondition3("liulu");
		QueryByCondition4("liulu");
//		 deleteRow("liulu","abcdef");
//		deleteByCondition("liulu", "abcdef");
	}

	public static void createTable(String tableNameStr) {
		System.out.println("start create table ......");
		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			Admin admin = connection.getAdmin();
			TableName tableName = TableName.valueOf(tableNameStr);
			if (admin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println(tableName + " is exist,detele....");
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			tableDescriptor.addFamily(new HColumnDescriptor("column1"));
			tableDescriptor.addFamily(new HColumnDescriptor("column2"));
			tableDescriptor.addFamily(new HColumnDescriptor("column3"));
			admin.createTable(tableDescriptor);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end create table ......");
	}

	public static void insertData(String tableName) {
		System.out.println("start insert data ......");
		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			Table table = connection.getTable(TableName.valueOf(tableName));
		
			Put put = new Put("112233bbbcccc".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
//			Put put = new Put("556677ggghhhh".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
			put.addColumn("column1".getBytes(), null, "aaa".getBytes());// 本行数据的第一列
			put.addColumn("column2".getBytes(), null, "bbb".getBytes());// 本行数据的第三列
			put.addColumn("column3".getBytes(), null, "ccc".getBytes());// 本行数据的第三列
		
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end insert data ......");
	}

	public static void dropTable(String tableName) {
		try {
			HBaseAdmin admin = new HBaseAdmin(configuration);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void deleteRow(String tableName, String rowkey) {
		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			Table table = connection.getTable(TableName.valueOf(tableName));
			
			List list = new ArrayList();
			Delete d1 = new Delete(rowkey.getBytes());
			list.add(d1);

			table.delete(list);
			System.out.println("删除行成功!");

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void deleteByCondition(String tablename, String rowkey) {
		// 目前还没有发现有效的API能够实现根据非rowkey的条件删除这个功能能，还有清空表全部数据的API操作

	}

	public static void QueryAll(String tableName) {
		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			Table table = connection.getTable(TableName.valueOf(tableName));
			
			ResultScanner rs = table.getScanner(new Scan());
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (Cell c : r.rawCells()) {
					 System.out.println(Bytes.toString(CellUtil.cloneRow(c))
						        + "==> " + Bytes.toString(CellUtil.cloneFamily(c))
						        + "{" + Bytes.toString(CellUtil.cloneQualifier(c))
						        + ":" + Bytes.toString(CellUtil.cloneValue(c)) + "}");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void QueryByCondition1(String tableName) {
		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			Table table = connection.getTable(TableName.valueOf(tableName));
			
			Get scan = new Get("112233bbbcccc".getBytes());// 根据rowkey查询
			Result r = table.get(scan);
			System.out.println(r.size());
			System.out.println("获得到rowkey:" + new String(r.getRow()));
			for (Cell c : r.rawCells()) {
				 System.out.println(Bytes.toString(CellUtil.cloneRow(c))
					        + "==> " + Bytes.toString(CellUtil.cloneFamily(c))
					        + "{" + Bytes.toString(CellUtil.cloneQualifier(c))
					        + ":" + Bytes.toString(CellUtil.cloneValue(c)) + "}");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void QueryByCondition2(String tableName) {

		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			Table table = connection.getTable(TableName.valueOf(tableName));
			
			Filter filter = new SingleColumnValueFilter(
					Bytes.toBytes("column1"), null, CompareOp.EQUAL,
					Bytes.toBytes("aaa")); // 当列column1的值为aaa时进行查询
			Scan s = new Scan();
			s.setFilter(filter);
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (Cell c : r.rawCells()) {
					 System.out.println(Bytes.toString(CellUtil.cloneRow(c))
						        + "==> " + Bytes.toString(CellUtil.cloneFamily(c))
						        + "{" + Bytes.toString(CellUtil.cloneQualifier(c))
						        + ":" + Bytes.toString(CellUtil.cloneValue(c)) + "}");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void QueryByCondition3(String tableName) {

		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			Table table = connection.getTable(TableName.valueOf(tableName));
			
			List<Filter> filters = new ArrayList<Filter>();

			Filter filter1 = new SingleColumnValueFilter(
					Bytes.toBytes("column1"), null, CompareOp.EQUAL,
					Bytes.toBytes("aaa"));
			filters.add(filter1);

			Filter filter2 = new SingleColumnValueFilter(
					Bytes.toBytes("column2"), null, CompareOp.EQUAL,
					Bytes.toBytes("bbb"));
			filters.add(filter2);

			Filter filter3 = new SingleColumnValueFilter(
					Bytes.toBytes("column3"), null, CompareOp.EQUAL,
					Bytes.toBytes("ccc"));
			filters.add(filter3);

			FilterList filterList1 = new FilterList(filters);

			Scan scan = new Scan();
			scan.setFilter(filterList1);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (Cell c : r.rawCells()) {
					 System.out.println(Bytes.toString(CellUtil.cloneRow(c))
						        + "==> " + Bytes.toString(CellUtil.cloneFamily(c))
						        + "{" + Bytes.toString(CellUtil.cloneQualifier(c))
						        + ":" + Bytes.toString(CellUtil.cloneValue(c)) + "}");
				}
			}
			rs.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 根据rowkey进行正则匹配
	 * @param tableName
	 */
	public static void QueryByCondition4(String tableName) {

		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			Table table = connection.getTable(TableName.valueOf(tableName));
			
//			RowFilter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("ggg"));

			FilterList filterListAll = new FilterList();
			RowFilter filter1 = new RowFilter(CompareOp.EQUAL, new SubstringComparator("55"));
			filterListAll.addFilter(filter1);
			
			RowFilter filter2 = new RowFilter(CompareOp.EQUAL, new SubstringComparator("ggg"));
			RowFilter filter3 = new RowFilter(CompareOp.EQUAL, new SubstringComparator("7777"));
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
			filterList.addFilter(filter2);
			filterList.addFilter(filter3);
			
			filterListAll.addFilter(filterList);
			
			Scan scan = new Scan();
			scan.setFilter(filterList);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				for (Cell c : r.rawCells()) {
					 System.out.println(Bytes.toString(CellUtil.cloneRow(c))
						        + "==> " + Bytes.toString(CellUtil.cloneFamily(c))
						        + "{" + Bytes.toString(CellUtil.cloneQualifier(c))
						        + ":" + Bytes.toString(CellUtil.cloneValue(c)) + "}");
				}
			}
			rs.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
