package idc.cloud.ex2.data;

import idc.cloud.ex2.AverageData;
import idc.cloud.ex2.StudentData;
import idc.cloud.ex2.TicketStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.spy.memcached.CASMutation;
import net.spy.memcached.CASMutator;
import net.spy.memcached.CachedData;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.LongTranscoder;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mysql.jdbc.Driver;

public class DataService {
	private static Log log = LogFactory.getLog(DataService.class);
	// private static final Logger log = Logger.getLogger(DataService.class);

	private static final StudentTranscoder STUDENT_TRANSCODER = new StudentTranscoder();
	private static final AverageTranscoder AVERAGE_TRANSCODER = new AverageTranscoder();

	private static final String AVERAGE_KEY = "avg-";
	private static final String STUDENT_KEY = "std-";
	private static final String TICKET_KEY = "tck-";
	private static final String VERSION_KEY = "vrs-";

	private MemcachedClient client;
	private ComboPooledDataSource cpds;
	private ExecutorService versionUpdateEx = Executors.newSingleThreadExecutor();
	private ExecutorService dbUpdateEx = Executors.newFixedThreadPool(10);

	public DataService() throws IOException {
		DbUtils.loadDriver(Driver.class.getName());
		cpds = new ComboPooledDataSource();
		String connString = System.getProperty("JDBC_CONNECTION_STRING",
				"jdbc:mysql://localhost:3306/mydb?user=yonatang&password=yonatang");
		cpds.setJdbcUrl(connString);

		try {
			QueryRunner run = new QueryRunner(cpds);
			run.update("CREATE TABLE IF NOT EXISTS students (student_id INT NOT NULL PRIMARY KEY, grade INT NOT NULL, version INT NOT NULL)");
			// Set up the sequence
			run.update("CREATE TABLE IF NOT EXISTS seq (seq_id INT NOT NULL PRIMARY KEY, num INT NOT NULL)");
			run.update("INSERT IGNORE INTO seq (seq_id, num) VALUES (?,?)", 0, 0);
		} catch (SQLException e) {
			log.error("Could not initiate database", e);
		}
		client = createCache();
	}

	public MemcachedClient unwrapCache() {
		return client;
	}

	public long getVersion() throws Exception {
		int retry = 0;
		while (true) {
			retry++;
			try {
				CASMutation<Long> mutation = new CASMutation<Long>() {
					@Override
					public Long getNewValue(Long current) {
						return ++current;
					}
				};
				CASMutator<Long> mutator = new CASMutator<Long>(client, new LongTranscoder());
				Long ad = mutator.cas(VERSION_KEY, null, 0, mutation);
				if (ad == null) {
					ad = mutator.cas(VERSION_KEY, getVersionDb() + 1000, 0, mutation);
				}
				if (ad % 1000 == 0) {
					final long dbValue = ad;
					versionUpdateEx.execute(new Runnable() {
						@Override
						public void run() {
							try {
								putVersionInDb(dbValue);
							} catch (SQLException e) {
							}
						}
					});
				}
				return ad;
			} catch (Exception e) {
				log.warn("Problem getting a version", e);
				if (retry > 5)
					throw e;
			}
		}
	}

	private long getVersionDb() throws SQLException {
		QueryRunner run = new QueryRunner(cpds);
		ResultSetHandler<Long> h = new ResultSetHandler<Long>() {
			@Override
			public Long handle(ResultSet rs) throws SQLException {
				if (rs.next()) {
					return rs.getLong(1);
				}
				return null;
			}
		};
		long num = run.query("SELECT num FROM seq WHERE seq_id=0", h);
		log.info("Done loading version backup from DB");
		return num;
	}

	private void putVersionInDb(long version) throws SQLException {
		Connection conn = null;

		PreparedStatement stmt0 = null;
		PreparedStatement stmt1 = null;
		ResultSet rs = null;
		int retry = 0;
		while (true) {
			retry++;
			try {
				conn = cpds.getConnection();
				conn.setAutoCommit(false);
				Long oldNum = null;
				stmt0 = conn.prepareStatement("SELECT num FROM seq WHERE seq_id = 0 FOR UPDATE");
				rs = stmt0.executeQuery();
				if (rs.next()) {
					oldNum = rs.getLong(1);
				}
				if (oldNum != null && oldNum > version) {
					version = oldNum;
				}
				stmt1 = conn
						.prepareStatement("INSERT INTO seq (num,seq_id) VALUES (?,0) ON DUPLICATE KEY UPDATE num=?");
				stmt1.setLong(1, version);
				stmt1.setLong(2, version);
				stmt1.executeUpdate();
				conn.commit();
				return;
			} catch (SQLException e) {
				DbUtils.rollbackAndCloseQuietly(conn);
				log.warn("Problem backing up the seq", e);
				if (retry > 5)
					throw e;
			} finally {
				DbUtils.closeQuietly(rs);
				DbUtils.closeQuietly(stmt1);
				DbUtils.closeQuietly(stmt0);
				DbUtils.closeQuietly(conn);
			}
		}
	}

	public StudentData updateStudentData(long id, final int grade) throws Exception {
		Preconditions.checkArgument(id > 0, "Id must be >0");
		Preconditions.checkArgument(grade >= 0, "Grade must be >=0");
		Preconditions.checkArgument(grade <= 100, "Grade must be <=100");
		final long version = getVersion();
		int retry = 0;
		while (true) {
			retry++;
			try {
				CASMutation<StudentData> mutation = new CASMutation<StudentData>() {
					public StudentData getNewValue(StudentData current) {
						if (current.getVersion() < version)
							return new StudentData(current.getGrade(), grade, version);
						return current;
					}
				};
				CASMutator<StudentData> mutator = new CASMutator<StudentData>(client, STUDENT_TRANSCODER);
				StudentData studentData = mutator.cas(STUDENT_KEY + id, new StudentData(grade, version), 0, mutation);
				return studentData;
			} catch (Exception e) {
				log.warn("Problem updating student " + id + " with grade " + grade + " on try " + retry, e);
				reloadStudentDataFromDb(id);
				if (retry > 5) {
					throw e;
				}
			}
		}
	}

	public AverageData updateAverageData(long id, final StudentData studentData) throws Exception {
		Preconditions.checkNotNull(studentData, "Student data mustn't be null");
		int retry = 0;
		while (true) {
			retry++;
			try {
				CASMutation<AverageData> mutation = new CASMutation<AverageData>() {
					@Override
					public AverageData getNewValue(AverageData current) {
						int delta = studentData.getGrade();
						boolean newStudent = true;
						if (studentData.getOldGrade() != null) {
							newStudent = false;
							delta -= studentData.getOldGrade();
						}
						return new AverageData(current.getCount() + (newStudent ? 1 : 0), current.getSum() + delta);
					}
				};
				CASMutator<AverageData> mutator = new CASMutator<AverageData>(client, AVERAGE_TRANSCODER);
				AverageData ad = mutator.cas(AVERAGE_KEY, null, 0, mutation);
				if (ad == null) {
					log.info("Average cache miss on adding student data " + studentData);
					ad = mutator.cas(AVERAGE_KEY, calcAverageFromDb(id, studentData), 0, mutation);
				}
				return ad;
			} catch (Exception e) {
				log.warn("Problem updating average with " + studentData + " to cache on try " + retry, e);
				if (retry > 5) {
					throw e;
				}
			}
		}
	}

	private AverageData calcAverageFromDb(long id, final StudentData studentData) throws SQLException {
		int retry = 0;
		while (true) {
			retry++;
			Connection conn = null;
			PreparedStatement stmt = null;
			ResultSet rs = null;
			try {
				conn = cpds.getConnection();
				conn.setAutoCommit(false);
				stmt = conn.prepareStatement("LOCK TABLES students WRITE");
				stmt.executeUpdate();
				stmt.close();
				stmt = conn.prepareStatement("SELECT grade,version FROM students WHERE student_id=?");
				stmt.setLong(1, id);
				rs = stmt.executeQuery();
				Integer oldGrade = null;
				if (rs.next()) {
					oldGrade = rs.getInt(1);
				}
				rs.close();
				stmt.close();
				stmt = conn.prepareStatement("SELECT COUNT(grade),SUM(grade) FROM students");
				rs = stmt.executeQuery();
				rs.next();
				int count = rs.getInt(1);
				long sum = rs.getLong(2);
				if (oldGrade == null) {
					count++;
					sum += studentData.getGrade();
				} else {
					sum += studentData.getGrade() - oldGrade;
				}
				rs.close();
				stmt = conn.prepareStatement("UNLOCK TABLES");
				stmt.executeUpdate();
				return new AverageData(count, sum);
			} catch (SQLException e) {
				log.warn("Problem while re-reading average from db");
				if (retry > 5)
					throw e;
			} finally {
				DbUtils.closeQuietly(conn, stmt, rs);
			}
		}
		// QueryRunner run = new QueryRunner(cpds);
		// ResultSetHandler<AverageData> h = new ResultSetHandler<AverageData>()
		// {
		// @Override
		// public AverageData handle(ResultSet rs) throws SQLException {
		// rs.next();
		// return new AverageData(rs.getInt(1) + 1, rs.getLong(2) +
		// studentData.getGrade());
		// }
		// };
		// AverageData averageData =
		// run.query("SELECT COUNT(grade), SUM(grade) FROM students;", h);
		// return averageData;
	}

	private AverageData calcAverageFromDb() throws SQLException {
		QueryRunner run = new QueryRunner(cpds);
		ResultSetHandler<AverageData> h = new ResultSetHandler<AverageData>() {
			@Override
			public AverageData handle(ResultSet rs) throws SQLException {
				rs.next();
				return new AverageData(rs.getInt(1), rs.getLong(2));
			}
		};
		AverageData averageData = run.query("SELECT COUNT(grade), SUM(grade) FROM students;", h);
		return averageData;
	}

	public AverageData getAverageData() throws Exception {
		int retry = 0;
		while (true) {
			retry++;
			try {
				AverageData ad = client.get(AVERAGE_KEY, AVERAGE_TRANSCODER);
				if (ad == null) {
					CASMutation<AverageData> mutation = new CASMutation<AverageData>() {
						@Override
						public AverageData getNewValue(AverageData current) {
							return current;
						}
					};
					CASMutator<AverageData> mutator = new CASMutator<AverageData>(client, AVERAGE_TRANSCODER);
					log.info("Average cache miss on reading from cache");
					ad = mutator.cas(AVERAGE_KEY, calcAverageFromDb(), 0, mutation);
				}
				return ad;
			} catch (Exception e) {
				log.warn("Problem while reading average from cache on try " + retry);
				if (retry > 5)
					throw e;
			}
		}
	}

	public StudentData getStudentFromCache(long id) throws Exception {
		int retry = 0;
		while (true) {
			retry++;
			try {
				return client.get(STUDENT_KEY + id, STUDENT_TRANSCODER);
			} catch (Exception e) {
				log.warn("Problem reading student " + id + " data from cache on try " + retry, e);
				reloadStudentDataFromDb(id);
				if (retry > 5) {
					throw e;
				}
			}
		}
	}

	private void reloadStudentDataFromDb(long id) throws Exception {
		int retry = 0;
		while (true) {
			retry++;
			try {
				QueryRunner run = new QueryRunner(cpds);
				ResultSetHandler<StudentData> rsh = new ResultSetHandler<StudentData>() {

					@Override
					public StudentData handle(ResultSet rs) throws SQLException {
						if (!rs.next())
							return null;
						return new StudentData(rs.getInt(1), rs.getLong(2));
					}
				};
				final StudentData sd = run.query("SELECT grade, version FROM students WHERE student_id = ?", rsh, id);
				if (sd == null) {
					log.debug("Student " + id + " is not in DB");
					return;
				}

				CASMutation<StudentData> mutation = new CASMutation<StudentData>() {
					public StudentData getNewValue(StudentData current) {
						if (current.getVersion() < sd.getVersion())
							return sd;
						return current;
					}
				};
				CASMutator<StudentData> mutator = new CASMutator<StudentData>(client, STUDENT_TRANSCODER);
				StudentData sdCache = mutator.cas(STUDENT_KEY + id, sd, 0, mutation);
				log.debug("Updated cache with student " + id + " " + sdCache);
			} catch (Exception e) {
				log.warn("Problem reading student id " + id + " on retry " + retry);
				if (retry > 5) {
					throw e;
				}
			}
		}
	}

	public String queueForDbUpdate(final long id, final StudentData studentData) throws Exception {
		final String ticket = RandomStringUtils.randomAlphanumeric(10);
		dbUpdateEx.execute(new Runnable() {

			@Override
			public void run() {
				int retry = 0;
				while (true) {
					retry++;
					Connection conn = null;
					PreparedStatement stmt = null;
					ResultSet rs = null;
					try {
						conn = cpds.getConnection();
						conn.setAutoCommit(false);
						stmt = conn.prepareStatement("SELECT version FROM students WHERE student_id = ? FOR UPDATE");
						stmt.setLong(1, id);
						rs = stmt.executeQuery();
						Long dbVersion = null;
						if (rs.next()) {
							dbVersion = rs.getLong(1);
						}
						rs.close();
						stmt.close();

						if (dbVersion == null) {
							stmt = conn
									.prepareStatement("INSERT INTO students (version,grade,student_id) VALUES (?,?,?)");
							stmt.setLong(1, studentData.getVersion());
							stmt.setInt(2, studentData.getGrade());
							stmt.setLong(3, id);
							stmt.executeUpdate();
						} else if (dbVersion < studentData.getVersion()) {
							stmt = conn.prepareStatement("UPDATE students SET version=?, grade=? WHERE student_id = ?");
							stmt.setLong(1, studentData.getVersion());
							stmt.setInt(2, studentData.getGrade());
							stmt.setLong(3, id);
							stmt.executeUpdate();
						} else {
							log.debug("Do nothing for " + id + " " + studentData);
						}
						if (studentData.getOldGrade() == null && dbVersion != null) {
							// cache thought it was first insertion, while it
							// already had something in db. In such cases,
							// reload of average is required.
							client.delete(AVERAGE_KEY).get();
						}
						conn.commit();
						client.set(TICKET_KEY + ticket, 120, TicketStatus.DONE);
						log.info("Ticket " + ticket + " is done: " + id + " " + studentData);
						return;
					} catch (Exception e) {
						if (retry > 5) {
							log.error("Ticket " + ticket + " couldn't update database with data of " + id + " "
									+ studentData);
							client.set(TICKET_KEY + ticket, 120, TicketStatus.FAILED);
							// update ticket;
						}
					} finally {
						DbUtils.closeQuietly(conn, stmt, rs);
					}
				}
			}
		});
		return ticket;
	}

	public TicketStatus getTicketStatus(String ticket) throws Exception {
		int retry = 0;
		while (true) {
			retry++;
			try {
				TicketStatus status = (TicketStatus) client.get(TICKET_KEY + ticket);
				if (status == null)
					return TicketStatus.PROCESSING;
				return status;
			} catch (Exception e) {
				log.warn("Problem checking ticket " + ticket + " on try " + retry);
				if (retry > 5)
					throw e;
			}
		}
	}

	public static MemcachedClient createCache() throws IOException {
		String cacheConnStr = System.getProperty("PARAM1", "localhost:11211");
		List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
		for (String conn : StringUtils.split(cacheConnStr, ' ')) {
			String[] parts = StringUtils.split(StringUtils.trim(conn), ':');
			if (parts.length != 2 || !StringUtils.isNumeric(parts[1])) {
				log.warn("Cannot understand cache host " + conn);
				continue;
			}
			int port;
			try {
				port = Integer.parseInt(parts[1]);
				if (port <= 0 || port > 65535)
					throw new RuntimeException();
			} catch (Exception e) {
				log.warn("Illegal port for cache host " + conn);
				continue;
			}
			addrs.add(new InetSocketAddress(parts[0], port));
		}
		MemcachedClient client = new MemcachedClient(addrs);
		log.info("Connected to the following cache servers " + addrs);
		return client;
	}

	private static class StudentTranscoder implements Transcoder<StudentData> {
		final SerializingTranscoder delegate = new SerializingTranscoder();

		public boolean asyncDecode(CachedData d) {
			return delegate.asyncDecode(d);
		}

		public StudentData decode(CachedData d) {
			return (StudentData) delegate.decode(d);
		}

		public CachedData encode(StudentData o) {
			return delegate.encode(o);
		}

		public int getMaxSize() {
			return delegate.getMaxSize();
		}
	}

	private static class AverageTranscoder implements Transcoder<AverageData> {
		final SerializingTranscoder delegate = new SerializingTranscoder();

		public boolean asyncDecode(CachedData d) {
			return delegate.asyncDecode(d);
		}

		public AverageData decode(CachedData d) {
			return (AverageData) delegate.decode(d);
		}

		public CachedData encode(AverageData o) {
			return delegate.encode(o);
		}

		public int getMaxSize() {
			return delegate.getMaxSize();
		}
	}

}
