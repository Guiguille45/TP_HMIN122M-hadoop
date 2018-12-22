
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Join {
	private static final String INPUT_PATH = "input-join-compact/";
	private static final String OUTPUT_PATH = "output/join-";
	private static final Logger LOG = Logger.getLogger(Join.class.getName());

	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

		try {
			FileHandler fh = new FileHandler("out.log");
			fh.setFormatter(new SimpleFormatter());
			LOG.addHandler(fh);
		} catch (SecurityException | IOException e) {
			System.exit(1);
		}
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] linecsv = line.split("\\|");

			String cle = "", val = "";
			if (linecsv.length == 8) { // CUSTOMERS
				cle = linecsv[0];
				val = "CUS;" + linecsv[1];
			} else if (linecsv.length == 9) { // ORDERS
				cle = linecsv[1];
				val = "ORD;" + linecsv[3];
			}
			
			context.write(new Text(cle), new Text(val));
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Double> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> orders = new ArrayList<String>();
			String name = "";
			
			for (Text val : values) {
				String value = val.toString();
				String[] vals = value.split(";");
				if (vals[0].equals("CUS")) {
					name = vals[1];
				} else if (vals[0].equals("ORD")) {
					orders.add(vals[1]);
				}
			}
			
			if (!name.equals("")) {
				double i = 0;
				for (String s : orders) {
					try {
						i += Double.parseDouble(s);
					} catch (NumberFormatException e) {
						System.out.println(s + " n'est pas un nombre valide");
					}
					
				}
				context.write(new Text(name), new Double(i));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "GroupBy");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}