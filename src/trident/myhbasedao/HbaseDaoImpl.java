package trident.myhbasedao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDaoImpl implements HbaseDao {
	HConnection hTablePool = null;
	Configuration conf = null;

	public HbaseDaoImpl() {

		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "mpc5,mpc6,mpc7");

		try {
			hTablePool = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void save(String tableName, String rowKey, String famliy,
			String qualifer, byte[] value) {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(famliy), Bytes.toBytes(qualifer), value);
		save(put, tableName);
	}

	@Override
	public void save(Put put, String tableName) {
		HTableInterface table = null;

		try {
			table = hTablePool.getTable(tableName);
			table.put(put);
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

	@Override
	public void save(List<Put> puts, String tableName) {
		HTableInterface table = null;

		try {
			table = hTablePool.getTable(tableName);
			table.put(puts);
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

	@Override
	public Result getOneRow(String tableName, String rowKey) {
		HTableInterface table = null;
		Result result = null;
		try {
			table = hTablePool.getTable(tableName);
			Get get = new Get(Bytes.toBytes(rowKey));
			result = table.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	@Override
	public List<Result> getRows(String tableName, String rowKey_like) {
		HTableInterface table = null;
		List<Result> list = new ArrayList<Result>();
		try {
			table = hTablePool.getTable(tableName);
			PrefixFilter filter = new PrefixFilter(Bytes.toBytes(rowKey_like));
			Scan scan = new Scan();
			scan.setFilter(filter);
			ResultScanner scanResult = table.getScanner(scan);
			for (Result rs : scanResult) {
				list.add(rs);
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
		return list;
	}

	@Override
	public void generateTable(String tableName, String... famlies) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			TableName tName = TableName.valueOf(tableName);
			HTableDescriptor td = new HTableDescriptor(tName);
			for (String famliy : famlies) {
				HColumnDescriptor cd = new HColumnDescriptor(famliy);
				cd.setMaxVersions(10);
				td.addFamily(cd);
			}
			admin.createTable(td);
			admin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void dropTable(String tableName) {
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			admin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void delete(String tableName, String rowKey, String family,
			String... qualifers) {
		HTableInterface table = null;

		try {
			table = hTablePool.getTable(tableName);
			Delete del = new Delete(Bytes.toBytes(rowKey));
			for (String qualifer : qualifers) {
				del.deleteColumns(Bytes.toBytes(family),
						Bytes.toBytes(qualifer));
			}
			table.delete(del);
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

	@Override
	public void save(String tableName, String rowKey, String famliy,
			Map<String, Object> map) {

		HTableInterface table = null;
		Put put = new Put(Bytes.toBytes(rowKey));

		Iterator<Entry<String, Object>> ito = map.entrySet().iterator();
		while (ito.hasNext()) {
			Entry<String, Object> entry = ito.next();
			put.add(Bytes.toBytes(famliy), Bytes.toBytes(entry.getKey()),
					Bytes.toBytes((String) entry.getValue()));
		}

		try {
			table = hTablePool.getTable(tableName);
			table.put(put);
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

	@Override
	public List<Result> getRows(String tableName, String rowKey_like,
			String family, String... qualifers) {
		HTableInterface table = null;
		List<Result> list = new ArrayList<Result>();
		try {
			table = hTablePool.getTable(tableName);
			PrefixFilter filter = new PrefixFilter(Bytes.toBytes(rowKey_like));
			Scan scan = new Scan();
			scan.setFilter(filter);
			for (String qualifer : qualifers) {
				scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifer));
			}
			ResultScanner scanResult = table.getScanner(scan);
			for (Result rs : scanResult) {
				list.add(rs);
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
		return list;
	}

	@Override
	public List<Result> getRows(String tableName, String startRow,
			String stopRow) {
		HTableInterface table = null;
		List<Result> list = new ArrayList<Result>();
		try {
			table = hTablePool.getTable(tableName);
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(startRow));
			scan.setStopRow(Bytes.toBytes(stopRow));
			ResultScanner scanResult = table.getScanner(scan);
			for (Result rs : scanResult) {
				list.add(rs);
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
		return list;
	}

	public static void main(String[] args) {
		HbaseDao dao = new HbaseDaoImpl();

		
		// for (int i = 1; i <= 5; i++) {
		// dao.delete("area_order", "2016-09-28_" + i, "cf", "order_amt");
		// }
		
		dao.dropTable("state");
		dao.generateTable("state", "cf");

		/*
		 * Result oneRow = dao.getOneRow("person_info", "rk_001");
		 * System.out.println(Bytes.toString(oneRow.getValue(
		 * Bytes.toBytes("base_info"), Bytes.toBytes("name"))));
		 * System.out.println("=========================="); for (Cell cell :
		 * oneRow.listCells()) { System.out.println("family:" +
		 * Bytes.toString(CellUtil.cloneFamily(cell)));
		 * System.out.println("qualifier:" +
		 * Bytes.toString(CellUtil.cloneQualifier(cell)));
		 * System.out.println("value:" +
		 * Bytes.toString(CellUtil.cloneValue(cell)));
		 * System.out.println("----------------------"); }
		 */

		// List<Result> rows = dao.getRows("person_info", "rk_001", "rk_007");
		// for (Result result : rows) {
		// for (Cell cell : result.listCells()) {
		// System.out.println("row:"
		// + Bytes.toString(CellUtil.cloneRow(cell)));
		// System.out.println("family:"
		// + Bytes.toString(CellUtil.cloneFamily(cell)));
		// System.out.println("qualifier:"
		// + Bytes.toString(CellUtil.cloneQualifier(cell)));
		// System.out.println("value:"
		// + Bytes.toString(CellUtil.cloneValue(cell)));
		// System.out.println("------------------");
		// }
		// }

		// dao.generateTable("area_order", "cf");
		// String tem ="123\tdddd\tfff";
		// System.out.println(StringUtils.split(tem, "\\t")[1]);
	}
}
