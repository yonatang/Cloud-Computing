package idc.cloud.ex2.data;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class VersionTest extends AbsDataTest {
	public void shouldGetVersion() throws Exception {
		Assert.assertEquals(ds.getVersion(), 1000);
		Assert.assertEquals(ds.getVersion(), 1001);
	}

	public void shouldGetVersionsTS() throws Exception {
		ExecutorService es = Executors.newFixedThreadPool(4);
		final int numberOfVersionsPerThread = 2000;
		final int numberOfThreads = 5;

		class MyRun implements Callable<List<Long>> {

			@Override
			public List<Long> call() throws Exception {
				List<Long> list = new ArrayList<Long>();
				for (int i = 0; i < numberOfVersionsPerThread; i++) {
					list.add(ds.getVersion());
				}
				return list;
			}

		}

		List<MyRun> runs = new ArrayList<MyRun>();
		for (int i = 0; i < numberOfThreads; i++) {
			runs.add(new MyRun());
		}
		List<Future<List<Long>>> fLists = es.invokeAll(runs);
		es.shutdown();
		Set<Long> allVersions = new HashSet<Long>();
		int count = 0;
		for (Future<List<Long>> fList : fLists) {
			List<Long> list = fList.get();
			long previous = -1;
			for (long val : list) {
				Assert.assertTrue(val > previous);
				previous = val;
			}
			count += list.size();
			allVersions.addAll(list);
		}
		Assert.assertEquals(allVersions.size(), count);
		Assert.assertEquals(count, numberOfThreads * numberOfVersionsPerThread);
	}

	public void shouldGetVersionsOnCacheFailure() throws Exception {
		long before = -1;
		for (int i = 0; i < 3000; i++) {
			long val = ds.getVersion();
			Assert.assertTrue(before < val);
			before = val;
			if (i == 1533) {
				daemon.stop();
				daemon.start();
			}
		}
	}

	public void shouldGetVersionsOnCacheEvict() throws Exception {
		long before = -1;
		for (int i = 0; i < 3000; i++) {
			long val = ds.getVersion();
			Assert.assertTrue(before < val);
			before = val;
			if (i == 1533) {
				ds.unwrapCache().flush().get();
			}
		}
	}

}
