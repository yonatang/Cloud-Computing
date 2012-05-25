package idc.cloud.ex2;

import idc.cloud.ex2.data.DataService;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import net.spy.memcached.MemcachedClient;

import org.apache.commons.dbutils.DbUtils;

import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;

public class TestUtils {

	public static void cleanDb() throws Exception {
		String connString = System.getProperty("JDBC_CONNECTION_STRING",
				"jdbc:mysql://localhost:3306/mydb?user=yonatang&password=yonatang");
		Connection conn = DriverManager.getConnection(connString);
		conn.prepareStatement("truncate table students").execute();
		conn.prepareStatement("truncate table seq").execute();
		DbUtils.closeQuietly(conn);
	}

	public static void waitForTickets(DataService ds, Collection<String> tickets) throws Exception {
		for (String ticket : tickets) {
			while (ds.getTicketStatus(ticket) == TicketStatus.PROCESSING) {
				Thread.sleep(100);
			}
		}
	}

	public static AverageData readAveragerFromDb() throws SQLException {
		String connString = System.getProperty("JDBC_CONNECTION_STRING",
				"jdbc:mysql://localhost:3306/mydb?user=yonatang&password=yonatang");
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement stmt = null;
		try {
			conn = DriverManager.getConnection(connString);
			stmt = conn.prepareStatement("SELECT COUNT(grade), SUM(grade) FROM students");
			rs = stmt.executeQuery();
			rs.next();
			return new AverageData(rs.getInt(1), rs.getLong(2));
		} finally {
			DbUtils.closeQuietly(conn, stmt, rs);
		}

	}

	public static StudentData readStudentDataFormDb(long id) throws SQLException {
		String connString = System.getProperty("JDBC_CONNECTION_STRING",
				"jdbc:mysql://localhost:3306/mydb?user=yonatang&password=yonatang");
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement stmt = null;
		try {
			conn = DriverManager.getConnection(connString);
			stmt = conn.prepareStatement("SELECT grade,version FROM students WHERE student_id=?");
			stmt.setLong(1, id);
			rs = stmt.executeQuery();
			if (!rs.next())
				return null;
			return new StudentData(rs.getInt(1), rs.getLong(2));
		} finally {
			DbUtils.closeQuietly(conn, stmt, rs);
		}
	}

	public static void clearCache() throws Exception {
		MemcachedClient client = DataService.createCache();
		client.flush().get();
		client.shutdown();
	}

	public static MemCacheDaemon<LocalCacheElement> getCacheServer(int port) {
		MemCacheDaemon<LocalCacheElement> daemon = new MemCacheDaemon<LocalCacheElement>();
		CacheStorage<Key, LocalCacheElement> storage = ConcurrentLinkedHashMap.create(
				ConcurrentLinkedHashMap.EvictionPolicy.FIFO, 1000000, 50 * 1024 * 1024);
		daemon.setCache(new CacheImpl(storage));
		daemon.setBinary(false);
		daemon.setAddr(new InetSocketAddress(port));
		daemon.setVerbose(false);
		return daemon;
	}
}
