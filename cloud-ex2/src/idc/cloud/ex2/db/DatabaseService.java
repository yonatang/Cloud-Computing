package idc.cloud.ex2.db;

import idc.cloud.ex2.Props;
import idc.cloud.ex2.cache.CacheService;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import com.google.common.base.Preconditions;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mysql.jdbc.Driver;

public class DatabaseService {

	private ComboPooledDataSource cpds;

	public DatabaseService() throws IOException {
		DbUtils.loadDriver(Driver.class.getName());
		cpds = new ComboPooledDataSource();
		Props p = Props.instance();
		cpds.setJdbcUrl(p.getJdbcUrl()); // "jdbc:mysql://mydbinstance.cykr0a1wxs2t.us-east-1.rds.amazonaws.com:3306/mydb");
		cpds.setUser(p.getJdbcUser());
		cpds.setPassword(p.getJdbcPass());

		try {
			QueryRunner run = new QueryRunner(cpds);
			run.update("CREATE TABLE IF NOT EXISTS students (student_id INT NOT NULL PRIMARY KEY, grade INT NOT NULL )");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void updateOrInsert(int id, int grade) throws SQLException {
		Preconditions.checkArgument(id > 0, "Id must be >0");
		Preconditions.checkArgument(grade >= 0, "Grade must be >=0");
		QueryRunner run = new QueryRunner(cpds);
		run.update("INSERT INTO students (student_id,grade) VALUES (?,?) ON DUPLICATE KEY UPDATE grade=?", id, grade,
				grade);
	}

	public Integer readGrade(int id) throws SQLException {
		Preconditions.checkArgument(id > 0, "Id must be >0");
		QueryRunner run = new QueryRunner(cpds);
		ResultSetHandler<Integer> h = new ResultSetHandler<Integer>() {

			@Override
			public Integer handle(ResultSet rs) throws SQLException {
				if (!rs.next()) {
					return null;
				}
				return rs.getInt("grade");
			}
		};
		Integer grade = run.query("SELECT grade FROM students WHERE student_id = ?", h, id);
		return grade;
	}

	public void reloadCacheWithIds(final CacheService cache) throws SQLException {
		QueryRunner run = new QueryRunner(cpds);
		ResultSetHandler<Void> h = new ResultSetHandler<Void>() {
			@Override
			public Void handle(ResultSet rs) throws SQLException {
				while (rs.next()) {
					Integer id = rs.getInt("student_id");
					Integer grade = rs.getInt("grade");
					if (id != null) {
						try {
							cache.addStudent(id, grade);
						} catch (Exception e) {
						}
					}
				}
				return null;
			}
		};
		run.query("SELECT student_id, grade FROM students", h);
		return;
	}

	public float getAvgFromDatabase() throws SQLException {
		QueryRunner run = new QueryRunner(cpds);
		ResultSetHandler<Float> h = new ResultSetHandler<Float>() {

			@Override
			public Float handle(ResultSet rs) throws SQLException {
				if (!rs.next()) {
					return null;
				}
				return rs.getFloat(1);
			}
		};
		Float grade = run.query("SELECT AVG(grade) FROM students;", h);
		return grade;
	}

	void deleteStudent(int studentId) throws SQLException {
		QueryRunner run = new QueryRunner(cpds);
		run.update("DELETE FROM students WHERE student_id = ?", studentId);
	}

	public int getStudentCount() throws SQLException {
		QueryRunner run = new QueryRunner(cpds);
		ResultSetHandler<Integer> h = new ResultSetHandler<Integer>() {
			@Override
			public Integer handle(ResultSet rs) throws SQLException {
				if (!rs.next()) {
					return 0;
				}
				return rs.getInt(1);
			}
		};
		return run.query("SELECT COUNT(*) FROM students", h);
	}
}
