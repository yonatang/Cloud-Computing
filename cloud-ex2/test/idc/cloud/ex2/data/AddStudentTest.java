package idc.cloud.ex2.data;

import idc.cloud.ex2.StudentData;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;

public class AddStudentTest extends AbsDataTest {
	public void shouldAddNewStudent() throws Exception {
		StudentData sd = ds.updateStudentData(1000, 100);
		Assert.assertEquals(sd.getGrade(), 100);
		Assert.assertEquals(sd.getOldGrade(), null);
		Assert.assertEquals(sd.getVersion(), 1000);
	}

	public void shouldUpdateStudent() throws Exception {
		StudentData sd = ds.updateStudentData(1000, 100);
		Assert.assertEquals(sd.getGrade(), 100);
		Assert.assertEquals(sd.getOldGrade(), null);
		Assert.assertEquals(sd.getVersion(), 1000);

		StudentData cacheSd = ds.getStudentFromCache(1000);
		Assert.assertEquals(cacheSd, sd);

		sd = ds.updateStudentData(1000, 90);
		Assert.assertEquals(sd.getGrade(), 90);
		Assert.assertEquals(sd.getOldGrade().intValue(), 100);
		Assert.assertEquals(sd.getVersion(), 1001);

		cacheSd = ds.getStudentFromCache(1000);
		Assert.assertEquals(cacheSd, sd);
	}

	public void shouldUpdateStudentsMT() throws Exception {
		final int amount = 1500;
		ExecutorService es = Executors.newFixedThreadPool(4);
		for (int i = 0; i < amount; i++) {
			es.execute(new Runnable() {
				@Override
				public void run() {
					try {
						ds.updateStudentData(rnd.nextInt(amount + amount / 4) + 1, rnd.nextInt(101));
					} catch (Exception e) {
						Assert.fail("", e);
					}
				}
			});
		}
		es.shutdown();
		es.awaitTermination(2, TimeUnit.MINUTES);
	}

}
