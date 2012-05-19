package idc.cloud.ex2;

import idc.cloud.ex2.cache.CacheException;
import idc.cloud.ex2.cache.CacheService;
import idc.cloud.ex2.cache.TicketStatus;
import idc.cloud.ex2.db.DatabaseService;

import java.security.SecureRandom;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.log4j.Logger;

public class BusinessLogic {
	private static Object[] lock = new Object[0];
	private static Logger log = Logger.getLogger(BusinessLogic.class);

	private CacheService cache;
	private DatabaseService db;
	private static final Random RANDOM = new SecureRandom();
	private static final ExecutorService es = Executors.newFixedThreadPool(50);

	private static BusinessLogic singleton;

	public static BusinessLogic instance() {
		synchronized (lock) {
			if (singleton == null) {
				singleton = new BusinessLogic();
			}
			return singleton;
		}
	}

	private void buildCache() {
		log.debug("Need to rebuild the cache");
		if (!cache.isLocked()) {
			log.info("Rebuilding the cache");
			// Rebuild cache
			new Thread(new Runnable() {
				@Override
				public void run() {
					// I don't mind having two instances doing it together.
					cache.lockCache();
					try {
						db.reloadCacheWithIds(cache);
					} catch (SQLException e) {
						// We're screwed.
						log.fatal("Could not reload cache.", e);
					}
					log.info("Cache rebuilt");
					cache.unlockCache();

				}
			}).start();
		} else {
			log.info("Cache is being rebuilt already");
		}

	}

	private BusinessLogic() {
		try {
			log.info("Initiating BusinessLogic");
			cache = new CacheService();
			db = new DatabaseService();
			if (cache.isMissingIdCache()) {
				buildCache();
			}
		} catch (Exception e) {
			log.error("Problem initiating BusinessLogic", e);
			// ??
		}
	}

	private void asyncOps(final String ticket, final int id, final int grade) throws Exception {
		final MutableBoolean mb = new MutableBoolean(true);
		Thread asyncAvg = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					log.debug("[" + ticket + "] Starting to calculate average asynchroniously");
					float avg = getAverage();
					log.debug("[" + ticket + "] Average caled: " + avg);
				} catch (Exception e) {
					log.error("Problem calcing the avg", e);
				}
			}
		});
		Thread asycDb = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					log.debug("[" + ticket + "] Starting to add to DB");
					db.updateOrInsert(id, grade);
					log.debug("[" + ticket + "] Added to db");
				} catch (Exception e) {
					log.error("Problem inserting to DB", e);
					mb.setValue(false);
				}

			}
		});
		asyncAvg.start();
		asycDb.start();
		asyncAvg.join();
		asycDb.join();
		cache.markTicket(ticket, mb.booleanValue());

	}

	public String addStudent(final int id, final int grade) throws Exception {
		// Secure random, to eliminate security breaches (such as session
		// guessing)
		final String ticket = RandomStringUtils.random(8, 0, 0, true, true, null, RANDOM);
		cache.addStudent(id, grade);
		es.execute(new Runnable() {
			@Override
			public void run() {
				try {
					asyncOps(ticket, id, grade);
				} catch (Exception e) {
					log.error("Ticket " + ticket + " for ID " + id + " and grade " + grade + " has failed", e);
				}
			}
		});
		// Thread async = new Thread();
		// async.start();
		return ticket;
	}

	public TicketStatus checkTicket(String ticket) throws Exception {
		return cache.ticketStatus(ticket);
	}

	public float getAverage() throws Exception {
		Float average = cache.getAverageFromCache();
		if (average != null && !average.isNaN()) {
			log.debug("Average cache hit: " + average);
			return average;
		}
		log.debug("Average cache miss");
		long sum = 0;
		int count = 0;
		try {
			Iterator<Integer> ids = cache.getStudentIds();
			while (ids.hasNext()) {
				count++;
				Integer id = ids.next();
				Integer grade = cache.readStudent(id);
				if (grade == null) {
					log.debug("Student miss");
					grade = db.readGrade(id);
					if (grade != null) {
						cache.addStudent(id, grade);
					} else {
						// fail and clear cache. Something is fishy.
						cache.flush();
						throw new CacheException("Unknwon ID encoutnered");

					}
				} else {
					sum += grade;
				}
			}
		} catch (CacheException e) {
			log.warn("Cache exception encountered. rebuilding student keys cache - " + e.getMessage());
			buildCache();
			// Meanwhile, fallback to database
			try {
				log.debug("Loading avg directly from db");
				return db.getAvgFromDatabase();
			} catch (SQLException e1) {
				// We're screwed.
				log.fatal("Could not calc average in db.", e1);
				throw e1;
			}
		}
		if (count == 0)
			return 0;
		average = (float) sum / count;
		cache.setAverageInCache(average);
		return average;
	}

}
