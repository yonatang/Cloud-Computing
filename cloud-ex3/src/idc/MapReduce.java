package idc;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MapReduce {

	public static class MapClass extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

		private Text word = new Text();

		@Override
		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			// key = site
			// value = site's content

			StringTokenizer st = new StringTokenizer(value.toString());
			while (st.hasMoreTokens()) {
				word.set(st.nextToken());
				output.collect(word, key);
			}
		}
	}

	public static class ReduceClass extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Set<String> sites = new HashSet<String>();
			while (values.hasNext()) {
				sites.add(values.next().toString());
			}
			output.collect(key, new Text(sites.toString()));
		}
	}

}
