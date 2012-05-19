package idc.cloud.ex2.cache;

import idc.cloud.ex2.Props;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import net.spy.memcached.CASMutation;
import net.spy.memcached.CASMutator;
import net.spy.memcached.CachedData;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.IntegerTranscoder;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

public class CacheService {

	static final String IDS_KEY = "ids-";
	private static final String STUDENTDS_KEY = "std-";
	private static final String AVG_KEY = "avg-";
	private static final String TICKET_KEY = "tck-";
	private static final String LOCK_KEY = "lck";
	static final StringTranscoder STRING_TRANSCODER = new StringTranscoder();
	static final IntegerTranscoder INTEGER_TRANSCODER = new IntegerTranscoder();
	static final FloatTranscoder FLOAT_TRANSCODER = new FloatTranscoder();
	static final Splitter SEMICOLUN_SPLITTER = Splitter.on(';').omitEmptyStrings();

	private MemcachedClient client;

	public CacheService() throws IOException {
		Props p = Props.instance();
		client = new MemcachedClient(new InetSocketAddress(p.getECHost(), p.getECPort()));
	}

	public boolean isMissingIdCache() {
		return client.get(IDS_KEY) == null;
	}

	public Iterator<Integer> getStudentIds() {
		if (isMissingIdCache())
			throw new CacheException("Missing ID cache");
		return new IdIterator(client);
	}

	public void addStudentId(int id) throws Exception {
		List<KeyValue> kvs = splitIdToLevels(id);
		for (KeyValue kv : kvs) {
			addToList(kv);
		}
	}

	public boolean isLocked() {
		return client.get(LOCK_KEY) != null;
	}

	public void lockCache() {
		client.add(LOCK_KEY, 120, true);
	}

	public void unlockCache() {
		client.delete(LOCK_KEY);
	}

	public void markTicket(String ticket, boolean val) throws Exception {
		client.add(TICKET_KEY + ticket, 60, (val == true) ? 1 : 0, INTEGER_TRANSCODER);
	}

	public TicketStatus ticketStatus(String ticket) throws Exception {
		Integer val = client.get(TICKET_KEY + ticket, INTEGER_TRANSCODER);
		if (val == null)
			return TicketStatus.PROCESSING;
		else if (val == 1)
			return TicketStatus.DONE;
		else
			return TicketStatus.FAILED;
	}

	public Integer readStudent(int id) {
		if (isLocked())
			throw new CacheException("Locked");
		return client.get(STUDENTDS_KEY + id, INTEGER_TRANSCODER);
	}

	public Float getAverageFromCache() {
		return client.get(AVG_KEY, FLOAT_TRANSCODER);
	}

	public void setAverageInCache(float average) {
		client.add(AVG_KEY, 0, average, FLOAT_TRANSCODER);
	}

	public void addStudent(int id, final int grade) throws Exception {
		Preconditions.checkArgument(id > 0, "ID must be > 0");

		CASMutation<Integer> mutation = new CASMutation<Integer>() {
			public Integer getNewValue(Integer current) {
				return grade;
			}
		};
		CASMutator<Integer> mutator = new CASMutator<Integer>(client, INTEGER_TRANSCODER);
		mutator.cas(STUDENTDS_KEY + id, grade, 0, mutation);
		client.delete(AVG_KEY);
		addStudentId(id);
	}

	List<KeyValue> splitIdToLevels(int id) {
		Preconditions.checkArgument(id > 0, "Id must be >0");
		List<KeyValue> result = new ArrayList<KeyValue>();
		String bucket = "";
		String soFar = "";
		while (id > 0) {
			bucket = bucket + (id % 10);
			id = id / 10;
			if (bucket.length() == 3) {
				KeyValue kv = new KeyValue(IDS_KEY + soFar, bucket);
				soFar = soFar + bucket;
				bucket = "";
				result.add(kv);
			}
		}
		bucket = bucket + "x";
		KeyValue kv = new KeyValue(IDS_KEY + soFar, bucket);
		result.add(kv);
		Collections.reverse(result); // Resistanced to non-atomic reads that way
		return result;
	}

	void addToList(final KeyValue kv) throws Exception {
		CASMutation<String> mutation = new CASMutation<String>() {
			public String getNewValue(String current) {
				if (current.contains(";" + kv.getValue() + ";"))
					return current;
				current = current + (kv.getValue() + ";");
				return current;
			}
		};
		CASMutator<String> mutator = new CASMutator<String>(client, STRING_TRANSCODER);
		mutator.cas(kv.getKey(), ";" + kv.getValue() + ";", 0, mutation);
	}

	public void flush() {
		client.flush();
	}

	static class StringTranscoder implements Transcoder<String> {
		final SerializingTranscoder delegate = new SerializingTranscoder();

		public boolean asyncDecode(CachedData d) {
			return delegate.asyncDecode(d);
		}

		public String decode(CachedData d) {
			return (String) delegate.decode(d);
		}

		public CachedData encode(String o) {
			return delegate.encode(o);
		}

		public int getMaxSize() {
			return delegate.getMaxSize();
		}
	}

	static class FloatTranscoder implements Transcoder<Float> {
		final SerializingTranscoder delegate = new SerializingTranscoder();

		public boolean asyncDecode(CachedData d) {
			return delegate.asyncDecode(d);
		}

		public Float decode(CachedData d) {
			return (Float) delegate.decode(d);
		}

		public CachedData encode(Float o) {
			return delegate.encode(o);
		}

		public int getMaxSize() {
			return delegate.getMaxSize();
		}
	}

}
