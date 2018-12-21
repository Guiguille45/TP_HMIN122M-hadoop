
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

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
//		private final static String[] lstKeys = {"Order Date", "State"};
//		private final static String[] lstKeys = {"Order Date", "Category"};
		private final static String[] lstKeys = {"Order ID"};
//		private final static String[] lstValues = {"Profit"};
		private final static String[] lstValues = {"Product ID", "Quantity"};
		private static ArrayList<Integer> indicesKeys = new ArrayList<Integer>();
//		private static Integer indiceValues = null;
		private static ArrayList<Integer> indicesValues = new ArrayList<Integer>();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] linecsv = line.split(",");
			
/*			// EXERCICE 3 - Group by sur Customer ID et profit
			int indKey = 5, indVal = 20;
			
			try {
			   Double profit = Double.parseDouble(linecsv[indVal]);
			   context.write(new Text(linecsv[indKey]), new DoubleWritable(profit));
			} catch (NumberFormatException e) {
				System.out.println(linecsv[indVal] + " n'est pas un nombre valide");
			}
*/			
/*			// EXERCICE 4 Date/State et Date/Categorie
			// si on ne dispose pas encore des indices des cles-valeurs a recuperer, alors on les recupere
			if (indicesKeys.size() == 0) {
				int i = 0;
				List<String> alstKeys = Arrays.asList(lstKeys);
				List<String> alstValues = Arrays.asList(lstValues);
				for (String val : linecsv) {
					if (alstKeys.contains(val)) { indicesKeys.add(i); }
					if (alstValues.contains(val)) { indiceValues = i; }
					i++;
				}
			// Si on dispose des indices des valeurs a recuperer
			} else {
				// Le try permet de verifier que la valeur est bien un nombre valide
				try {
					String strKeys = "";
					for (Integer i : indicesKeys) {
						strKeys += linecsv[i] + ";";
					}
					
				   Double profit = Double.parseDouble(linecsv[indiceValues]);
				   context.write(new Text(strKeys.substring(0, strKeys.length()-1)), new DoubleWritable(profit));
				} catch (NumberFormatException e) {
					System.out.println(linecsv[indiceValues] + " n'est pas un nombre valide");
				}
			}
*/
			// EXERCICE 4 
			// si on ne dispose pas encore des indices des cles-valeurs a recuperer, alors on les recupere
			if (indicesKeys.size() == 0) {
				int i = 0;
				List<String> alstKeys = Arrays.asList(lstKeys);
				List<String> alstValues = Arrays.asList(lstValues);
				for (String val : linecsv) {
					if (alstKeys.contains(val)) { indicesKeys.add(i); }
					if (alstValues.contains(val)) { indicesValues.add(i); }
					i++;
				}
			// Si on dispose des indices des valeurs a recuperer
			} else {
				String strKeys = "";
				for (Integer i : indicesKeys) {
					strKeys += linecsv[i] + ";";
				}
				String strValues = "";
				for (Integer i : indicesValues) {
					strValues += linecsv[i] + ";";
				}
				
				context.write(new Text(strKeys.substring(0, strKeys.length()-1)), new Text(strValues.substring(0, strValues.length()-1)));
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// EXERCIE 3 et 4 Date/State et Date/Categorie
/*			Double sum = 0d;

			for (Text val : values)
				sum += val.toString();
			
			context.write(key, new DoubleWritable(sum));
*/
			int nbProduct = 0, nbex = 0;
			ArrayList<String> productID = new ArrayList<String>();
			
			for (Text val : values) {
				String value = val.toString();
				String[] vals = value.split(";");
				if (!productID.contains(vals[0])) {
					productID.add(vals[0]);
					nbProduct += 1;
				}
				try {
					nbex += Integer.parseInt(vals[1]);
				} catch (NumberFormatException e) {
					System.out.println(vals[1] + " n'est pas un nombre valide");
				}
			}
			
			context.write(key, new Text(nbProduct + "\t" + nbex));
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