package idc.cloud.ex2;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Props {
	private static Object[] lock = new Object[0];
	private static Props singelton;

	public static Props instance() throws IOException {
		synchronized (lock) {
			if (singelton == null) {
				singelton = new Props();
			}
			return singelton;
		}
	}

	private Properties p;

	private Props() throws IOException {
		p = new Properties();
		InputStream is = null;
		try {
			is = this.getClass().getResourceAsStream("/servers.properties");
			if (is == null) {
				is = this.getClass().getResourceAsStream("servers.properties");
			}
			p.load(is);
		} finally {
			if (is != null) {
				is.close();
			}
		}
	}

	public String getJdbcUrl() {
		return p.getProperty("jdbc.url");
	}

	public String getJdbcUser() {
		return p.getProperty("jdbc.username");
	}

	public String getJdbcPass() {
		return p.getProperty("jdbc.password");
	}

	public String getECHost() {
		return p.getProperty("elasticache.host");
	}

	public int getECPort() {
		return Integer.parseInt(p.getProperty("elasticache.port"));
	}
}
