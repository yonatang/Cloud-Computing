package idc.cloud.ex2.data;

import idc.cloud.ex2.AverageData;
import idc.cloud.ex2.StudentData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class CalcAvgTest extends AbsDataTest {
	public void shouldCalcAvgOnAddMT() throws Exception {

		class MyRun implements Runnable {
			private int start;
			private List<Integer> list = new ArrayList<Integer>();

			MyRun(int start) {
				this.start = start;
			}

			@Override
			public void run() {
				try {
					for (int i = start; i < start + 1000; i++) {
						int grade = rnd.nextInt(101);
						list.add(grade);
						StudentData sd = ds.updateStudentData(i + 1000, grade);
						ds.updateAverageData(i + 1000, sd);
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
		int expCount = 0;
		int expSum = 0;
		for (MyRun run : runs) {
			expCount += run.list.size();
			for (int grade : run.list) {
				expSum += grade;
			}
		}
		Assert.assertEquals(cacheData.getSum(), expSum);
		Assert.assertEquals(cacheData.getCount(), expCount);
	}

	public void shouldCalcAvgOnUpdateMT() throws Exception {
		class MyRun implements Runnable {
			private int start;
			private Map<Long, Integer> map = new HashMap<Long, Integer>();

			MyRun(int start) {
				this.start = start;
			}

			@Override
			public void run() {
				try {
					for (int i = start; i < start + 1000; i++) {
						int grade = rnd.nextInt(101);
						long id = rnd.nextInt(500) + start;
						map.put(id, grade);
						StudentData sd = ds.updateStudentData(id + 1000, grade);
						ds.updateAverageData(id + 1000, sd);
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
		int expCount = 0;
		int expSum = 0;
		for (MyRun run : runs) {
			expCount += run.map.size();
			for (int grade : run.map.values()) {
				expSum += grade;
			}
		}
		Assert.assertEquals(cacheData.getSum(), expSum);
		Assert.assertEquals(cacheData.getCount(), expCount);
	}

	public void shouldCalcAvgOnNewAdd() throws Exception {
		StudentData sd = ds.updateStudentData(1000, 100);
		AverageData avg = ds.updateAverageData(1000, sd);
		Assert.assertEquals(avg, new AverageData(1, 100));
	}

	public void shouldCalcAvgOnMultipleAdd() throws Exception {
		StudentData sd = ds.updateStudentData(1000, 100);
		AverageData avg = ds.updateAverageData(1000, sd);
		sd = ds.updateStudentData(1001, 90);
		avg = ds.updateAverageData(1001, sd);
		Assert.assertEquals(avg, new AverageData(2, 190));
	}

	public void shouldCalcAvgOnUpdates() throws Exception {
		StudentData sd = ds.updateStudentData(1000, 100);
		AverageData avg = ds.updateAverageData(1000, sd);
		sd = ds.updateStudentData(1001, 90);
		avg = ds.updateAverageData(1001, sd);
		sd = ds.updateStudentData(1000, 90);
		avg = ds.updateAverageData(1000, sd);
		Assert.assertEquals(avg, new AverageData(2, 180));
	}

}
