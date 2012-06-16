package idc.cloud.ex3.mapred;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class Runner {
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Runner.class);
		conf.setJobName("Runner");
		
		conf.setMapperClass(MapReduce.MapClass.class);
		conf.setCombinerClass(MapReduce.ReduceClass.class);
		conf.setReducerClass(MapReduce.ReduceClass.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(HtmlFileTextInput.class);
		conf.setOutputFormat(CsvFileOutput.class);

		File outputDir=new File("./output");
		outputDir.mkdirs();
		FileUtils.deleteDirectory(outputDir);
		FileInputFormat.setInputPaths(conf, new Path("./input"));
		FileOutputFormat.setOutputPath(conf, new Path("./output"));

		JobClient.runJob(conf);

	}
}
