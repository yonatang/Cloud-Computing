package idc.cloud.ex2;

import java.io.IOException;
import java.net.InetSocketAddress;

import net.spy.memcached.MemcachedClient;

public class TestUtils {

	public static void clearCache() throws IOException {
		String cacheServer = System.getProperty("cache.host", "localhost");
		int cachePort = Integer.parseInt(System.getProperty("cache.port", "11211"));
		MemcachedClient client = new MemcachedClient(new InetSocketAddress(cacheServer, cachePort));
		client.flush();
		client.shutdown();
	}
}
