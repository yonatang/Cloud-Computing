package idc.cloud.ex2;

import idc.cloud.ex2.data.DataService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BusinessLogic {
	private static Object[] lock = new Object[0];
	private static Log log = LogFactory.getLog(BusinessLogic.class); // Logger.getLogger(BusinessLogic.class.getName());

	private DataService dataService;

	private static BusinessLogic singleton;

	public static BusinessLogic instance() {
		synchronized (lock) {
			if (singleton == null) {
				singleton = new BusinessLogic();
			}
			return singleton;
		}
	}

	private BusinessLogic() {
		try {
			log.info("Initiating BusinessLogic");
			dataService = new DataService();
			log.info("Done");
		} catch (Exception e) {
			log.error("Problem initiating BusinessLogic", e);
		}
	}

	public String addStudent(final long id, final int grade) throws Exception {
		StudentData studentData = dataService.updateStudentData(id, grade);
		dataService.updateAverageData(id, studentData);
		String ticket = dataService.queueForDbUpdate(id, studentData);
		log.info("Added " + id + ", " + studentData + " to cache, got ticket " + ticket);
		return ticket;
	}

	public TicketStatus checkTicket(String ticket) throws Exception {
		return dataService.getTicketStatus(ticket);
	}

	public float getAverage() throws Exception {
		AverageData averageData = dataService.getAverageData();
		log.info("Got average data " + averageData + " - " + averageData.getAverage());
		return averageData.getAverage();
	}

	public AverageData getAverageData() throws Exception {
		AverageData averageData = dataService.getAverageData();
		log.info("Got average data " + averageData + " - " + averageData.getAverage());
		return averageData;
	}

}
