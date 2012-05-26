package idc.cloud.ex2.rest;

import idc.cloud.ex2.BusinessLogic;
import idc.cloud.ex2.TicketStatus;

import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@Path("/students")
public class RestService {
	private static Log log = LogFactory.getLog(RestService.class);
	private final BusinessLogic bl = BusinessLogic.instance();

	@GET
	@Path("average/{RANDOM}")
	@Produces(MediaType.APPLICATION_JSON)
	public String getAverage() {
		log.info("Get average");
		try {
			float average = bl.getAverage();
			log.info("Calced avg " + average);
			return RestResponse.succeeded(average).asJson();
		} catch (Exception e) {
			log.error("Error while updating grade", e);
			return RestResponse.failed(e.getMessage()).asJson();
		}
	}

	@POST
	@Path("update")
	@Produces(MediaType.APPLICATION_JSON)
	public String updateGrade(@FormParam("studentId") int id, @FormParam("grade") int grade) {
		log.info("Update grade for student " + id + " with grade " + grade);
		try {
			String ticket = bl.addStudent(id, grade);
			return RestResponse.succeeded(ticket).asJson();
		} catch (Exception e) {
			log.error("Error while updating grade", e);
			return RestResponse.failed(e.getMessage()).asJson();
		}
	}

	// Random is needed due to IE caching.
	@GET
	@Path("ticket/{ID}/{RANDOM}")
	@Produces(MediaType.APPLICATION_JSON)
	public String getTicketStatus(@PathParam("ID") String ticket) {
		TicketStatus ticketStatus = null;
		try {
			ticketStatus = bl.checkTicket(ticket);
			return RestResponse.succeeded(ticketStatus).asJson();
		} catch (Exception e) {
			log.error("Checking ticket " + ticket + " failed", e);
			return RestResponse.failed(e.getMessage()).asJson();
		}
	}

}
