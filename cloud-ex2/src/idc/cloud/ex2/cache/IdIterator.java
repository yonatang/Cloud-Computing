package idc.cloud.ex2.cache;

import java.util.ArrayList;
import java.util.Iterator;

import net.spy.memcached.MemcachedClient;

import com.google.common.collect.Lists;

public class IdIterator implements Iterator<Integer> {
	private MemcachedClient client;
	private boolean hasMore = true;
	private final int LEVELS = 10;
	@SuppressWarnings("unchecked")
	private ArrayList<String>[] lists = new ArrayList[LEVELS];
	private int[] cursors = new int[LEVELS];
	private String[] listKey = new String[LEVELS];

	public IdIterator(MemcachedClient client) {
		this.client = client;
		String root = client.get(CacheService.IDS_KEY, CacheService.STRING_TRANSCODER);
		if (root == null || root.length() == 0) {
			hasMore = false;
			return;
		}
		listKey[0] = "";
		lists[0] = Lists.newArrayList(CacheService.SEMICOLUN_SPLITTER.split(root));
		cursors[0] = 0;
		for (int i = 1; i < LEVELS; i++) {
			lists[i] = null;
			cursors[i] = -1;
		}
	}

	@Override
	public boolean hasNext() {
		return hasMore;
	}

	private boolean inc(int c) {
		if (c == -1) {
			return false;
		}
		if (cursors[c] + 1 < lists[c].size()) {
			cursors[c]++;
			return true;
		} else {
			lists[c] = null;
			return inc(c - 1);
		}
	}

	@Override
	public Integer next() {
		int c = 0;
		String[] vals = new String[LEVELS];
		boolean notFound = true;
		int retry = 0;
		while (notFound) {
			while (true) {
				if (lists[c] == null) {
					String thisList = client.get(CacheService.IDS_KEY + listKey[c], CacheService.STRING_TRANSCODER);
					if (thisList == null || thisList.length() == 0) {
						throw new CacheException("Cache flushed");
					}

					lists[c] = Lists.newArrayList(CacheService.SEMICOLUN_SPLITTER.split(thisList));
					cursors[c] = 0;
				}
				if (lists[c].size() <= cursors[c]) {
					lists[c] = null;
					if (retry == 5) {
						throw new CacheException("Cache flushed");
					}
					// Inconsist cache state. Retry.
					retry++;
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
					}
					break;
				}
				retry = 0;
				vals[c] = lists[c].get(cursors[c]);
				if (vals[c].endsWith("x")) {
					hasMore = inc(c);
					notFound = false;
					break;
				} else {
					c++;
					listKey[c] = listKey[c - 1] + vals[c - 1];
				}
			}
		}
		StringBuilder sb = new StringBuilder();
		for (String v : vals) {
			if (v == null)
				break;
			sb.append(v);
		}
		sb.reverse();
		String next = sb.toString();
		next = next.substring(1, next.length());
		return Integer.parseInt(next);
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Remove is not supported");
	}

}
