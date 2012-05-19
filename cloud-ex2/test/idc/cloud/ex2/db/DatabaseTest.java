package idc.cloud.ex2.db;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class DatabaseTest {

	public void smokeTest() throws Exception {
		DatabaseService db = new DatabaseService();
	}

	public void shouldInsertStudent() throws Exception {
		DatabaseService db = new DatabaseService();
		int studentId = 40645509;
		db.deleteStudent(studentId);
		Assert.assertNull(db.readGrade(studentId));
		db.updateOrInsert(studentId, 99);
		Assert.assertEquals(db.readGrade(studentId).intValue(), 99);
		db.updateOrInsert(studentId, 98);
		Assert.assertEquals(db.readGrade(studentId).intValue(), 98);
	}
}
