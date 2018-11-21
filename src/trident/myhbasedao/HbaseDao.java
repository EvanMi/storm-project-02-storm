package trident.myhbasedao;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public interface HbaseDao {

	void generateTable(String tableName, String... famlies);

	void dropTable(String tableName);

	void delete(String tableName, String rowKey, String family,
			String... qualifers);

	void save(String tableName, String rowKey, String famliy, String qualifer,
			byte[] value);

	void save(String tableName, String rowKey, String famliy,
			Map<String, Object> map);

	void save(Put put, String tableName);

	void save(List<Put> puts, String tableName);

	Result getOneRow(String tableName, String rowKey);

	List<Result> getRows(String tableName, String rowKey_like);

	List<Result> getRows(String tableName, String rowKey_like, String family,
			String... qualifers);

	List<Result> getRows(String tableName, String startRow, String stopRow);

}
