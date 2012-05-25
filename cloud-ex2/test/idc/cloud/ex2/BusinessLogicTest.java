package idc.cloud.ex2;

import idc.cloud.ex2.data.DataService;

import java.util.Random;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;

@Test
public class BusinessLogicTest {

	private BusinessLogic bl;

	MemCacheDaemon<LocalCacheElement> daemon;

	@BeforeClass
	void setupCacheServer() {
		int port1 = new Random().nextInt(40000) + 10000;
		daemon = TestUtils.getCacheServer(port1);

		daemon.start();

		System.setProperty("PARAM1", "localhost:" + port1);
	}

	@AfterClass(alwaysRun = true)
	void teardownCacheServer() {
		if (daemon != null)
			daemon.stop();
	}

	@BeforeMethod
	void setup() throws Exception {
		TestUtils.clearCache();
		TestUtils.cleanDb();
		bl = BusinessLogic.instance();
		new DataService();
	}

	public void shouldInsertFast() throws Exception {
		bl.addStudent(12345, 80);
		long now = System.currentTimeMillis();
		// First one is slow
		String ticket = bl.addStudent(12346, 80);
		while (bl.checkTicket(ticket) == TicketStatus.PROCESSING) {
			Thread.yield();
		}
		long duration = System.currentTimeMillis() - now;
		Assert.assertTrue(duration < 90);
	}

	public void shouldWorkWithNoDuplicationsCleanDb() throws Exception {
		final Random random = new Random();
		final int amount = 1000;
		long now = System.currentTimeMillis();
		for (int i = 0; i < amount; i++) {
			int grade = random.nextInt(101);
			long id = 10000 + random.nextInt(amount * amount * amount);
			bl.addStudent(id, grade);
		}
		long duration = System.currentTimeMillis() - now;
		Thread.sleep(1000);

		summary(amount, duration);
	}

	public void shouldWorkWithNoDupFailedConn() throws Exception {
		final Random random = new Random();
		final int amount = 1000;
		long now = System.currentTimeMillis();
		for (int i = 0; i < amount; i++) {
			int grade = random.nextInt(101);
			long id = 10000 + i;
			bl.addStudent(id, grade);
		}
		Thread.sleep(1000);
		daemon.stop();
		daemon.start();

		for (int i = 0; i < amount; i++) {
			int grade = random.nextInt(101);
			long id = 10000 + i;
			bl.addStudent(id, grade);
		}
		Thread.sleep(1000);

		long duration = System.currentTimeMillis() - now;
		Thread.sleep(1000);
		summary(amount, duration);

	}

	private void summary(int amount, long duration) throws Exception {
		System.out.println("Added " + amount + " students in " + duration + " ms");
		System.out.println("Cache average is " + bl.getAverage() + ". Rate of insertion is "
				+ ((float) amount / duration * 1000) + " records per seconds");
		AverageData cAvg = bl.getAverageData();
		AverageData dbAvg = TestUtils.readAveragerFromDb();
		System.out.println("Database average is " + dbAvg);
		System.out.println("Cache average data is " + cAvg);
		Assert.assertEquals(cAvg, dbAvg);
	}

	public void shouldWorkWithDups() throws Exception {
		final int amount = 50;
		long now = System.currentTimeMillis();
		for (int i = 0; i < amount; i++) {
			int grade = i;
			long id = 10000 + (i / 2);
			bl.addStudent(id, grade);
		}
		long duration = System.currentTimeMillis() - now;
		Thread.sleep(1000);
		summary(amount, duration);
	}

	public void shouldWorkWithRandomInserts() throws Exception {
		final Random random = new Random();
		final int amount = 1000;
		long now = System.currentTimeMillis();
		for (int i = 0; i < amount; i++) {
			int grade = random.nextInt(101);
			long id = 10000 + random.nextInt(amount + amount / 4);
			bl.addStudent(id, grade);
		}
		long duration = System.currentTimeMillis() - now;
		Thread.sleep(1000);
		summary(amount, duration);
	}

	public void shouldRecoverFromCacheDisconnection() throws Exception {
		final Random random = new Random();
		final int amount = 5;
		long now = System.currentTimeMillis();
		for (int i = 0; i < amount; i++) {
			int grade = random.nextInt(101);
			long id = 10000 + random.nextInt(amount + amount / 4);
			bl.addStudent(id, grade);
		}
		Thread.sleep(1000);
		Logger.getRootLogger().info("!!!!!!!!! Restarting the cache");
		daemon.stop();
		daemon.start();
		for (int i = 0; i < amount; i++) {
			int grade = random.nextInt(101);
			long id = 10000 + random.nextInt(amount + amount / 4);
			bl.addStudent(id, grade);
		}
		long duration = System.currentTimeMillis() - now;
		Thread.sleep(1000);
		summary(amount, duration);
	}

	public void shouldCalcAverage() throws Exception {
		final int amount = 51;
		long now = System.currentTimeMillis();

		long oldid = -1;
		for (int j = 0; j < 2; j++) {
			for (int i = 0; i < amount; i++) {
				int grade = i + (j * 50); // random.nextInt(101);
				long id = 10000 + i;
				if (i == 25) {
					id = oldid;
				}
				oldid = id;
				bl.addStudent(id, grade);
			}
			if (j == 0) {
				daemon.stop();
				daemon.start();
			}
		}
		long duration = System.currentTimeMillis() - now;
		Thread.sleep(500);
		summary(amount, duration);
	}
}
