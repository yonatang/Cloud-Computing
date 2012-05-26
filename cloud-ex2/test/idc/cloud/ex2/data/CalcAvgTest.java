package idc.cloud.ex2.data;

import idc.cloud.ex2.AverageData;
import idc.cloud.ex2.StudentData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class CalcAvgTest extends AbsDataTest {
	public void shouldCalcAvgOnAddMT() throws Exception {

		final int studetnsPerThread = 1000;
		final int executors = 10;

		class MyRun implements Callable<List<Integer>> {
			private int start;

			MyRun(int start) {
				this.start = start;
			}

			@Override
			public List<Integer> call() throws Exception {
				List<Integer> list = new ArrayList<Integer>();
				for (int i = start; i < start + studetnsPerThread; i++) {
					int grade = rnd.nextInt(101);
					list.add(grade);
					StudentData sd = ds.updateStudentData(i + 1000, grade);
					ds.updateAverageData(i + 1000, sd);
				}
				return list;
			}
		}

		ExecutorService es = Executors.newFixedThreadPool(4);
		List<MyRun> runs = new ArrayList<MyRun>();
		for (int i = 0; i < 10; i++) {
			MyRun run = new MyRun(i * studetnsPerThread * 2);
			runs.add(run);
		}
		List<Future<List<Integer>>> fListInts = es.invokeAll(runs);
		es.shutdown();

		AverageData cacheData = ds.getAverageData();
		int expCount = 0;
		int expSum = 0;
		for (Future<List<Integer>> fListInt : fListInts) {
			List<Integer> ints = fListInt.get();
			expCount += ints.size();
			for (int grade : ints) {
				expSum += grade;
			}
		}
		Assert.assertEquals(cacheData.getSum(), expSum);
		Assert.assertEquals(cacheData.getCount(), expCount);
		Assert.assertEquals(expCount, studetnsPerThread * executors);
	}

	public void shouldCalcAvgOnUpdateMT() throws Exception {
		class MyRun implements Callable<Map<Long, Integer>> {
			private int start;

			MyRun(int start) {
				this.start = start;
			}

			@Override
			public Map<Long, Integer> call() throws Exception {
				Map<Long, Integer> map = new HashMap<Long, Integer>();
				for (int i = start; i < start + 1000; i++) {
					int grade = rnd.nextInt(101);
					long id = rnd.nextInt(500) + start;
					map.put(id, grade);
					StudentData sd = ds.updateStudentData(id + 1000, grade);
					ds.updateAverageData(id + 1000, sd);
				}
				return map;
			}
		}

		ExecutorService es = Executors.newFixedThreadPool(4);
		List<MyRun> runs = new ArrayList<MyRun>();
		for (int i = 0; i < 10; i++) {
			MyRun run = new MyRun(i * 2000);
			runs.add(run);
		}
		List<Future<Map<Long, Integer>>> fMaps = es.invokeAll(runs);
		es.shutdown();

		AverageData cacheData = ds.getAverageData();
		int expCount = 0;
		int expSum = 0;
		for (Future<Map<Long, Integer>> fMap : fMaps) {
			Map<Long, Integer> map = fMap.get();
			expCount += map.size();
			for (int grade : map.values()) {
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
