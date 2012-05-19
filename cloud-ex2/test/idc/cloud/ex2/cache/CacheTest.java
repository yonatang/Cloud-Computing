package idc.cloud.ex2.cache;

import idc.cloud.ex2.BusinessLogic;
import idc.cloud.ex2.TestUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;

@Test
public class CacheTest {
	MemCacheDaemon<LocalCacheElement> daemon;

	public void shouldCacheWork() throws Exception {
		TestUtils.clearCache();
		CacheService cs = new CacheService();
		cs.addStudentId(1234);
		cs.addStudentId(1233);
		Thread.sleep(1000);
		Iterator<Integer> i = cs.getStudentIds();
		long now = System.currentTimeMillis();
		while (i.hasNext()) {
			Integer id = i.next();
			System.out.println(id);
			Assert.assertNotNull(id);
		}
		System.out.println("Took " + now + " ms");
		// System.out.println(cs.getStudentIds());
	}

	public void shouldWorkWithLargeSizes() throws Exception {
		TestUtils.clearCache();
		CacheService cs = new CacheService();
		int SIZE = 15000;

		long now = System.currentTimeMillis();
		for (int i = 1; i < SIZE + 1; i++) {
			if (i % 1000 == 0) {
				System.out.println(i + " in " + (System.currentTimeMillis() - now));
				now = System.currentTimeMillis();
			}
			cs.addStudentId(100000000 + i);
		}
		System.out.println("DONE");
		Iterator<Integer> i = cs.getStudentIds();
		int count = 0;
		now = System.currentTimeMillis();
		while (i.hasNext()) {
			count++;
			Integer id = i.next();
			Assert.assertNotNull(id);
		}
		long duration = System.currentTimeMillis() - now;
		System.out.println("Took " + duration + " ms");
		Assert.assertEquals(count, SIZE);
	}

	public void shouldWorkMultiThreaded() throws Exception {
		TestUtils.clearCache();
		final CacheService cs = new CacheService();
		Thread.sleep(500);
		for (int i = 1; i < 15000; i++) {
			cs.addStudentId(100000000 + i);
		}
		System.out.println("DONE");

		Thread t1 = new Thread(new Runnable() {
			@Override
			public void run() {
				CacheService cs;
				Random rnd = new Random();
				try {
					cs = new CacheService();
					for (int i = 0; i < 3000; i++) {
						cs.addStudentId(100000000 + rnd.nextInt(49999999));
					}
				} catch (Exception e) {
					e.printStackTrace();
					return;
				}

			}
		});
		Thread t2 = new Thread(new Runnable() {

			@Override
			public void run() {
				CacheService cs;
				try {
					cs = new CacheService();
					Iterator<Integer> i = cs.getStudentIds();
					int count = 0;
					while (i.hasNext()) {
						count++;
						System.out.println(i.next());
					}
					System.out.println("COUNTER " + count);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});

		t1.start();
		t2.start();
		t1.join();
		t2.join();
	}

	public void shouldBucketWork() throws Exception {
		TestUtils.clearCache();
		CacheService cs = new CacheService();
		List<KeyValue> kvs;

		kvs = cs.splitIdToLevels(1);
		Assert.assertEquals(kvs.size(), 1);
		Assert.assertEquals(kvs.get(0), new KeyValue("ids-", "1x"));

		kvs = cs.splitIdToLevels(40283304);
		Assert.assertEquals(kvs.size(), 3);
		Assert.assertEquals(kvs.get(0), new KeyValue("ids-", "403"));
		Assert.assertEquals(kvs.get(1), new KeyValue("ids-403", "382"));
		Assert.assertEquals(kvs.get(2), new KeyValue("ids-403382", "04x"));

		kvs = cs.splitIdToLevels(40283300);
		Assert.assertEquals(kvs.size(), 3);
		Assert.assertEquals(kvs.get(0), new KeyValue("ids-", "003"));
		Assert.assertEquals(kvs.get(1), new KeyValue("ids-003", "382"));
		Assert.assertEquals(kvs.get(2), new KeyValue("ids-003382", "04x"));

		kvs = cs.splitIdToLevels(140283304);
		Assert.assertEquals(kvs.size(), 4);
		Assert.assertEquals(kvs.get(0), new KeyValue("ids-", "403"));
		Assert.assertEquals(kvs.get(1), new KeyValue("ids-403", "382"));
		Assert.assertEquals(kvs.get(2), new KeyValue("ids-403382", "041"));
		Assert.assertEquals(kvs.get(3), new KeyValue("ids-403382041", "x"));
	}

	public void shouldAddStudent() throws Exception {
		TestUtils.clearCache();
		CacheService cs = new CacheService();
		cs.addStudent(36563305, 80);
		Iterator<Integer> students = cs.getStudentIds();
		Assert.assertTrue(students.hasNext());
		int id = students.next();
		Assert.assertFalse(students.hasNext());
		Assert.assertEquals(id, 36563305);
		Assert.assertEquals(cs.readStudent(36563305), (Integer) 80);
	}

	public void shouldCalcAverage() throws Exception {
		TestUtils.clearCache();
		BusinessLogic bl = BusinessLogic.instance();
		CacheService cs = new CacheService();
		Random random = new Random();
		long sum = 0;
		for (int i = 0; i < 5000; i++) {
			int grade = random.nextInt(61) + 40;
			sum += grade;
			int id = random.nextInt(399999999);
			cs.addStudent(id, grade);
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
