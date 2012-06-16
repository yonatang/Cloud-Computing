package idc.cloud.ex3.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class HtmlFileTextInput extends FileInputFormat<Text, Text> implements
		JobConfigurable {
	public void configure(JobConf conf) {
	}

	protected boolean isSplitable(FileSystem fs, Path file) {
		return false;
	}

	public RecordReader<Text, Text> getRecordReader(InputSplit genericSplit,
			JobConf job, Reporter reporter) throws IOException {
		FileSplit fs = (FileSplit) genericSplit;
		reporter.setStatus(genericSplit.toString());
		return new HtmlRecordReader(job, fs);
	}
}
