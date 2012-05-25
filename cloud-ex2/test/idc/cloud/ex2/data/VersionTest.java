package idc.cloud.ex2.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
		final List<Long> versions = Collections.synchronizedList(new ArrayList<Long>());
		Runnable r = new Runnable() {

			@Override
			public void run() {
				long before = Long.MIN_VALUE;
				for (int i = 0; i < 2000; i++) {
					try {
						long val = ds.getVersion();
						versions.add(val);
						Assert.assertTrue(before < val);
						before = val;
					} catch (Exception e) {
						Assert.fail("", e);
					}
				}
			}
		};
		for (int i = 0; i < 5; i++) {
			es.execute(r);
		}
		es.shutdown();
		es.awaitTermination(2, TimeUnit.MINUTES);
		Assert.assertEquals(new HashSet<Long>(versions).size(), versions.size());
		Assert.assertEquals(versions.size(), 5 * 2000);
	}

	public void shouldGetVersionsOnCacheFailure() throws Exception {
		long before = Long.MIN_VALUE;
		for (int i = 0; i < 3000; i++) {
			long val = ds.getVersion();
			Assert.assertTrue(before < val);
			before = val;
			if (i == 1533) {
				daemon1.stop();
				daemon1.start();
			}
		}
	}

	public void shouldGetVersionsOnCacheEvict() throws Exception {
		long before = Long.MIN_VALUE;
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
