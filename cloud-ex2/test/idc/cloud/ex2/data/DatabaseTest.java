package idc.cloud.ex2.data;

import idc.cloud.ex2.AverageData;
import idc.cloud.ex2.StudentData;
import idc.cloud.ex2.TestUtils;
import idc.cloud.ex2.TicketStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class DatabaseTest extends AbsDataTest {

	public void shouldAddToDb() throws Exception {
		StudentData sd = ds.updateStudentData(1000, 100);
		AverageData ad = ds.updateAverageData(1000, sd);
		Assert.assertEquals(ad, new AverageData(1, 100));
		String ticket = ds.queueForDbUpdate(1000, sd);
		while (ds.getTicketStatus(ticket) == TicketStatus.PROCESSING) {
			Thread.yield();
		}
		Assert.assertEquals(ds.getTicketStatus(ticket), TicketStatus.DONE);
		StudentData dbSd = TestUtils.readStudentDataFormDb(1000);
		Assert.assertEquals(sd.getGrade(), dbSd.getGrade());
		Assert.assertEquals(sd.getVersion(), dbSd.getVersion());
	}

	public void shouldUpdateDb() throws Exception {
		StudentData sd = ds.updateStudentData(1000, 100);
		ds.updateAverageData(1000, sd);
		String ticket1 = ds.queueForDbUpdate(1000, sd);

		sd = ds.updateStudentData(1000, 90);
		ds.updateAverageData(1000, sd);
		String ticket2 = ds.queueForDbUpdate(1000, sd);

		while (ds.getTicketStatus(ticket1) == TicketStatus.PROCESSING) {
			Thread.yield();
		}
		while (ds.getTicketStatus(ticket2) == TicketStatus.PROCESSING) {
			Thread.yield();
		}
		Assert.assertEquals(ds.getTicketStatus(ticket2), TicketStatus.DONE);
		Assert.assertEquals(ds.getTicketStatus(ticket1), TicketStatus.DONE);
		StudentData dbSd = TestUtils.readStudentDataFormDb(1000);
		Assert.assertEquals(sd.getGrade(), dbSd.getGrade());
		Assert.assertEquals(sd.getVersion(), dbSd.getVersion());
	}

	public void shouldUpdateDbMT() throws Exception {
		class MyRun implements Runnable {
			private int start;

			Map<Long, Integer> map = new HashMap<Long, Integer>();
			List<String> tickets = new ArrayList<String>();

			MyRun(int start) {
				this.start = start;
			}

			@Override
			public void run() {
				try {
					for (int i = start; i < start + 1000; i++) {
						int grade = rnd.nextInt(101);
						long id = rnd.nextInt(500) + start + 1000;
						map.put(id, grade);
						StudentData sd = ds.updateStudentData(id, grade);
						ds.updateAverageData(id, sd);
						String ticket = ds.queueForDbUpdate(id, sd);
						tickets.add(ticket);
					}
				} catch (Exception e) {
					Assert.fail("", e);
				}
			}
		}

		ExecutorService es = Executors.newFixedThreadPool(4);
		List<MyRun> runs = new ArrayList<MyRun>();
		for (int i = 0; i < 10; i++) {
			MyRun run = new MyRun(i * 2000);
			runs.add(run);
			es.execute(run);
		}
		es.shutdown();
		es.awaitTermination(2, TimeUnit.MINUTES);

		AverageData cacheData = ds.getAverageData();
		System.out.println(cacheData);
		for (MyRun run : runs) {
			TestUtils.waitForTickets(ds, run.tickets);
		}
		int expCount = 0;
		int expSum = 0;
		System.out.println("Verifing database. Might take a minute of two.");
		for (MyRun run : runs) {
			expCount += run.map.size();
			for (Entry<Long, Integer> entry : run.map.entrySet()) {
				expSum += entry.getValue();
				StudentData dbSd = TestUtils.readStudentDataFormDb(entry.getKey());
				Assert.assertNotNull(dbSd);
				Assert.assertEquals(dbSd.getGrade(), entry.getValue().intValue());
			}
		}
		Assert.assertEquals(cacheData.getSum(), expSum);
		Assert.assertEquals(cacheData.getCount(), expCount);
	}

	public void shouldAddMT() throws Exception {
		class MyRun implements Callable<List<String>> {

			@Override
			public List<String> call() throws Exception {
				List<String> tickets = new ArrayList<String>();
				try {
					for (int i = 0; i < 1000; i++) {
						int grade = rnd.nextInt(101);
						long id = i + 1000;
						StudentData sd = ds.updateStudentData(id, grade);
						ds.updateAverageData(id, sd);
						String ticket = ds.queueForDbUpdate(id, sd);
						tickets.add(ticket);
					}
				} catch (Exception e) {
					Assert.fail("", e);
				}
				return tickets;
			}
		}

		List<MyRun> runs = new ArrayList<MyRun>();
		ExecutorService es = Executors.newFixedThreadPool(4);
		for (int i = 0; i < 4; i++) {
			MyRun r = new MyRun();
			runs.add(r);
		}

		System.out.println("Wating to complete submittion");
		List<Future<List<String>>> fTicketLists = es.invokeAll(runs);
		es.shutdown();
		for (Future<List<String>> fTicketList : fTicketLists) {
			List<String> tickets = fTicketList.get();
			System.out.println("Waiting for a thread to complete DB operation");
			TestUtils.waitForTickets(ds, tickets);
		}
		Assert.assertEquals(ds.getAverageData(), TestUtils.readAveragerFromDb());
	}

	public void shouldResistCacheFailures() throws Exception {
		List<String> tickets = new ArrayList<String>();
		Map<Long, Integer> map = new HashMap<Long, Integer>();
		for (int i = 0; i < 4000; i++) {
			int grade = rnd.nextInt(101);
			long id = rnd.nextInt(1000) + 1000;
			if (i % 456 == 0) {
				daemon.stop();
				daemon.start();
				tickets.clear();
			}
			map.put(id, grade);
			StudentData sd = ds.updateStudentData(id, grade);
			ds.updateAverageData(id, sd);
			String ticket = ds.queueForDbUpdate(id, sd);
			tickets.add(ticket);
		}

		TestUtils.waitForTickets(ds, tickets);
		AverageData cacheData = ds.getAverageData();

		Assert.assertEquals(cacheData, TestUtils.readAveragerFromDb());

		int expSum = 0;
		for (Entry<Long, Integer> entry : map.entrySet()) {
			expSum += entry.getValue();
		}

		Assert.assertEquals(cacheData.getCount(), map.size());
		Assert.assertEquals(cacheData.getSum(), expSum);
		System.out.println(cacheData);
		System.out.println("Map size: " + map.size());

	}

	public void shouldResistCacheExpiration() throws Exception {
		List<String> tickets = new ArrayList<String>();
		Map<Long, Integer> map = new HashMap<Long, Integer>();
		for (int i = 0; i < 4000; i++) {
			int grade = rnd.nextInt(101);
			long id = rnd.nextInt(1000) + 1000;
			if (i % 458 == 0) {
				ds.unwrapCache().flush().get();
				tickets.clear();
			}
			map.put(id, grade);
			StudentData sd = ds.updateStudentData(id, grade);
			ds.updateAverageData(id, sd);
			String ticket = ds.queueForDbUpdate(id, sd);
			tickets.add(ticket);
		}

		TestUtils.waitForTickets(ds, tickets);
		AverageData cacheData = ds.getAverageData();

		Assert.assertEquals(cacheData, TestUtils.readAveragerFromDb());

		int expSum = 0;
		for (Entry<Long, Integer> entry : map.entrySet()) {
			expSum += entry.getValue();
		}

		Assert.assertEquals(cacheData.getCount(), map.size());
		Assert.assertEquals(cacheData.getSum(), expSum);

	}

}
