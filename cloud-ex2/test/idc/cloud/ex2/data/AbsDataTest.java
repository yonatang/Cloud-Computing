package idc.cloud.ex2.data;

import idc.cloud.ex2.TestUtils;

import java.util.Random;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;

public abstract class AbsDataTest {
	DataService ds;
	Random rnd = new Random();
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
		daemon.stop();
	}

	@BeforeMethod
	void setup() throws Exception {
		TestUtils.clearCache();
		TestUtils.cleanDb();
		ds = new DataService();
	}
}
