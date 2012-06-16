package idc.cloud.ex3.mapred;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import au.com.bytecode.opencsv.CSVWriter;

public class CsvFileOutput extends FileOutputFormat<Text, Text> {

	@Override
	public RecordWriter<Text, Text> getRecordWriter(FileSystem ignored,
			JobConf job, String name, Progressable progress) throws IOException {
		Path file = FileOutputFormat.getTaskOutputPath(job, name + ".csv");

		FileSystem fs = file.getFileSystem(job);
		FSDataOutputStream fileOut = fs.create(file, progress);

		CsvRecordWriter cro = new CsvRecordWriter(fileOut);
		return cro;
	}

	static class CsvRecordWriter implements RecordWriter<Text, Text> {
		private CSVWriter csvWriter;

		public CsvRecordWriter(DataOutputStream out) {
			this.csvWriter = new CSVWriter(new OutputStreamWriter(out));
		}

		@Override
		public void close(Reporter reporter) throws IOException {
			IOUtils.closeQuietly(csvWriter);
		}

		@Override
		public void write(Text key, Text value) throws IOException {
			List<String> cols = new ArrayList<String>();
			String keyStr = key.toString();
			cols.add(keyStr);
			boolean onlyCount = false;
			int count = 0;
			if (StringUtils.startsWithIgnoreCase(keyStr, "http://")
					|| StringUtils.startsWithIgnoreCase(keyStr, "https://")) {
				onlyCount = true;
			}
			String valueStr = value.toString();
			StringTokenizer st = new StringTokenizer(valueStr);
			while (st.hasMoreTokens()) {
				count++;
				String site = st.nextToken();
				if (!onlyCount) {
					cols.add(site);
				}
			}
			if (onlyCount) {
				cols.add(String.valueOf(count));
			}
			csvWriter.writeNext(cols.toArray(new String[0]));
			csvWriter.flush();
		}

	}

}
