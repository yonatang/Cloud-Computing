package idc.cloud.ex2.rest;

import com.google.gson.Gson;

public class RestResponse {

	public static RestResponse failed(String reason) {
		RestResponse r = new RestResponse();
		r.succeeded = false;
		r.reason = reason;
		return r;
	}

	public static RestResponse succeeded(Object value) {
		RestResponse r = new RestResponse();
		r.succeeded = true;
		r.value = value;
		return r;
	}

	public static RestResponse succeeded() {
		RestResponse r = new RestResponse();
		r.succeeded = true;
		return r;
	}

	private RestResponse() {
	}

	private String reason;
	private boolean succeeded;
	private Object value;

	public String getReason() {
		return reason;
	}

	public void setReason(String reason) {
		this.reason = reason;
	}

	public boolean isSucceeded() {
		return succeeded;
	}

	public void setSucceeded(boolean succeeded) {
		this.succeeded = succeeded;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public String asJson() {
		return new Gson().toJson(this);
	}
}
