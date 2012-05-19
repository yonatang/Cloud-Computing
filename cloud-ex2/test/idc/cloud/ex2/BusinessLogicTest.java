package idc.cloud.ex2;

import idc.cloud.ex2.cache.CacheService;
import idc.cloud.ex2.cache.TicketStatus;

import java.util.Random;

import org.testng.annotations.Test;

@Test
public class BusinessLogicTest {

	public void shouldInsertFast() throws Exception {
		BusinessLogic bl = BusinessLogic.instance();
		CacheService cache = new CacheService();
		TestUtils.clearCache();
		String ticket = bl.addStudent(12345, 80);
		long now = System.currentTimeMillis();
		System.out.println("Inserted, got ticket " + ticket);
		System.out.println("Took " + (System.currentTimeMillis() - now));
		while (cache.ticketStatus(ticket) == TicketStatus.PROCESSING) {
			Thread.sleep(100);
		}
		System.out.println("Done async in " + (System.currentTimeMillis() - now));
		System.out.println(bl.getAverage());

	}

	public void shouldCalcAverage() throws Exception {
		BusinessLogic bl = BusinessLogic.instance();
		CacheService cs = new CacheService();
		Random random = new Random();
		long sum = 0;
		for (int i = 0; i < 2500; i++) {
			int grade = random.nextInt(61) + 40;
			Thread.sleep(100);
			sum += grade;
			int id = random.nextInt(399999999);
			bl.addStudent(id, grade);
		}
		System.out.println("Added students");
		System.out.println("My calc is " + ((float) sum / 5000));
		System.out.print("His calc is");
		long now = System.currentTimeMillis();
		float avg = bl.getAverage();
		long duration = System.currentTimeMillis() - now;
		System.out.println(" " + avg + " in " + duration + " ms");
		// again
		now = System.currentTimeMillis();
		avg = bl.getAverage();
		duration = System.currentTimeMillis() - now;
		System.out.println(" " + avg + " in " + duration + " ms");
		cs.addStudent(2312313, 50);
		sum += 50;
		now = System.currentTimeMillis();
		avg = bl.getAverage();
		duration = System.currentTimeMillis() - now;
		System.out.println(" " + avg + " in " + duration + " ms");

	}
}
