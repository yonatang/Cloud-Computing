package idc.cloud.ex2.rest;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Application;

public class StudentApp extends Application {
	public Set<Class<?>> getClasses() {
		HashSet<Class<?>> classes = new HashSet<Class<?>>();
		classes.add(RestService.class);
		return classes;

	}
}