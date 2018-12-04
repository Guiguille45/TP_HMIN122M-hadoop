
import java.io.IOException;
import java.time.Instant;
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

public class GroupBy {
	private static final String INPUT_PATH = "input-groupBy/";
	private static final String OUTPUT_PATH = "output/groupBy-";
	private static final Logger LOG = Logger.getLogger(GroupBy.class.getName());

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

	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
//		private final static String[] lstKeys = {"Customer ID"};
//		private final static String[] lstValues = {"Profit"};
//		private static ArrayList<Integer> indiceKeys = new ArrayList<Integer>();
//		private static ArrayList<Integer> indiceValues = new ArrayList<Integer>();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] linecsv = line.split(",");
			int indKey = 5, indVal = 20;
			
			try {
			   Double profit = Double.parseDouble(linecsv[indVal]);
			   context.write(new Text(linecsv[indKey]), new DoubleWritable(profit));
			} catch (NumberFormatException e) {
				System.out.println(linecsv[indVal] + " n'est pas un nombre valide");
			}
		}
	}
	
	
	
	// si on ne dispose pas encore des indices des cles-valeurs a recuperer, alors on les recupere
//	if (indiceKeys.size() == 0) {
//		int i = 0;
//		List<String> alstKeys = Arrays.asList(lstKeys);
//		List<String> alstValues = Arrays.asList(lstValues);
//		for (String val : linecsv) {
//			if (alstKeys.contains(val)) { indiceKeys.add(i); }
//			if (alstValues.contains(val)) { indiceValues.add(i); }
//			i++;
//		}
//	} else {
//		
//		context.write(new Text(""), new Float(""));
//	}

	public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			Double sum = 0d;

			for (DoubleWritable val : values)
				sum += val.get();
			
			context.write(key, new DoubleWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "GroupBy");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}