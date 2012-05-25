package idc.cloud.ex2.data;

import idc.cloud.ex2.TicketStatus;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TicketTest extends AbsDataTest {
	public void shouldCheckTickets() throws Exception {
		Assert.assertEquals(ds.getTicketStatus("blabla"), TicketStatus.PROCESSING);
	}

}
