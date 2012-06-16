package idc.cloud.ex3.mapred;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class HtmlRecordReader implements RecordReader<Text, Text> {

	private String siteName;
	private FSDataInputStream fileIn;
	private boolean processed = false;

	public HtmlRecordReader(JobConf job, FileSplit split) throws IOException {

		Path file = split.getPath();
		siteName = StringUtils.replace(file.getName().toLowerCase()," ","%20");
		FileSystem fs = file.getFileSystem(job);
		fileIn = fs.open(split.getPath());
	}

	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(fileIn);
	}

	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		return (processed ? 1 : 0);
	}

	@Override
	public float getProgress() throws IOException {
		return (processed ? 1.0F : 0.0F);
	}

	@Override
	public boolean next(Text key, Text value) throws IOException {
		if (processed)
			return false;

		Document doc = Jsoup.parse(fileIn, null, "http://" + siteName);
		String docText = doc.text();

		key.set(siteName);
		Elements elems = doc.getElementsByTag("a");
		StringBuilder sb = new StringBuilder();
		for (Element e : elems) {
			String url = e.attr("href");
			// Ignore inner links
			if (StringUtils.startsWithIgnoreCase(url, "http://")
					|| StringUtils.startsWithIgnoreCase(url, "https://")) {
				url = StringUtils.replace(url, " ", "%20");
				sb.append(' ');
				sb.append(url);
			}
		}
		docText += sb.toString();
		value.set(docText);
		processed = true;
		return true;
	}

}
