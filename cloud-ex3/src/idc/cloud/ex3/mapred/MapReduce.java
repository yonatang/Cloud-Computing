package idc.cloud.ex3.mapred;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MapReduce {

	private static final String IGNORE_CHARS = ",.'\"~?!@#$%^&*()_+`-=;:[{}]/|\u201D\u201C";

	static enum Counters {
		INPUT_WORDS
	};

	public static class MapClass extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		private Text word = new Text();

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Text site = key;
			StringTokenizer st = new StringTokenizer(value.toString());
			while (st.hasMoreTokens()) {
				String wordStr = StringUtils.defaultString(st.nextToken())
						.toLowerCase();
				if (!StringUtils.startsWithIgnoreCase(wordStr, "http://")
						&& !StringUtils.startsWithIgnoreCase(wordStr,
								"https://")) {
					wordStr = StringUtils.replaceChars(wordStr, IGNORE_CHARS,
							"");
				} else {
					String host = "";
					try {
						host = new URL(wordStr).getHost();
					} catch (MalformedURLException e) {
					}
					wordStr = "http://" + host;
				}
				word.set(wordStr);
				if (word.getLength() > 0) {
					output.collect(word, new Text(site));
				}
				reporter.incrCounter(Counters.INPUT_WORDS, 1);
			}
		}
	}

	public static class ReduceClass extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Set<String> sites = new HashSet<String>();
			while (values.hasNext()) {
				sites.add(values.next().toString());
			}
			output.collect(key, new Text(StringUtils.join(sites, ' ')));
		}
	}

}
