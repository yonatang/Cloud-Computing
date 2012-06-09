package idc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Runner {
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Runner.class);
		conf.setJobName("Runner");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(MapReduce.MapClass.class);
		conf.setCombinerClass(MapReduce.ReduceClass.class);
		conf.setReducerClass(MapReduce.ReduceClass.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path("./site1"));
		FileOutputFormat.setOutputPath(conf, new Path("./output"));

		JobClient.runJob(conf);

	}
}
