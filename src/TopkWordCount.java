
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/*
 * Jusqu'à présent nous avons défini nos mappers et reducers comme des classes internes à notre classe principale.
 * Dans des applications réelles de map-reduce cela ne sera généralement pas le cas, les classes seront probablement localisées dans d'autres fichiers.
 * Dans cet exemple, nous avons défini Map et Reduce en dehors de notre classe principale.
 * Il se pose alors le problème du passage du paramètre 'k' dans notre reducer, car il n'est en effet plus possible de déclarer un paramètre k dans notre classe principale qui serait partagé avec ses classes internes ; c'est la que la Configuration du Job entre en jeu.
 */

// =========================================================================
// MAPPER
// =========================================================================

class Map extends Mapper<LongWritable, Text, Text, Text> {
	private final static IntWritable one = new IntWritable(1);
	private final static String emptyWords[] = { "" };

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		String[] words = line.split(",");

		if (Arrays.equals(words, emptyWords))
			return;

		context.write(new Text(words[0]), new Text(words[20]));
	}
}

// =========================================================================
// REDUCER
// =========================================================================

class Reduce extends Reducer<Text, Text, Text, Text> {
	/**
	 * Map avec tri suivant l'ordre naturel de la clé (la clé représentant la fréquence d'un ou plusieurs mots).
	 * Utilisé pour conserver les k mots les plus fréquents.
	 * 
	 * Il associe une fréquence à une liste de mots.
	 */
	private TreeMap<Double, List<Text>> sortedWords = new TreeMap<>(Collections.reverseOrder());
	private int k;
	private int line = 0;

	/**
	 * Méthode appelée avant le début de la phase reduce.
	 */
	@Override
	public void setup(Context context) {
		// On charge k
		k = context.getConfiguration().getInt("k", 1);
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String valeur = "";
		for (Text val : values)
			valeur = val.toString();

		Double profit = (double) 0;
		try {
		   profit = Double.parseDouble(valeur);
		} catch (NumberFormatException e) {
			System.out.println(valeur + " n'est pas un nombre valide");
		}
		
		// On copie car l'objet key reste le même entre chaque appel du reducer
		Text keyCopy = new Text(key);
				
		if (sortedWords.containsKey(profit))
			sortedWords.get(profit).add(keyCopy);
		else {
			List<Text> words = new ArrayList<>();
			words.add(keyCopy);
			sortedWords.put(profit, words);
		}
	}

	/**
	 * Méthode appelée à la fin de l'étape de reduce.
	 * 
	 * Ici on envoie les mots dans la sortie, triés par ordre descendant.
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Double s : sortedWords.keySet()) {
			List<Text> ids = sortedWords.get(s);
			
			for (Text t : ids) {
				if (line < k) {
					context.write(t, new Text(s.toString()));
					line++;
				}
			}
		}
		
		
		
//		Integer[] nbofs = sortedWords.keySet().toArray(new Integer[0]);
//
//		// Parcours en sens inverse pour obtenir un ordre descendant
//		int i = nbofs.length;
//
//		while (i-- != 0) {
//			Integer nbof = nbofs[i];
//
//			for (Text words : sortedWords.get(nbof))
//				context.write(words, new IntWritable(nbof));
//		}
	}
}

public class TopkWordCount {
	private static final String INPUT_PATH = "input-groupBy/";
	private static final String OUTPUT_PATH = "output/TopkWordCount-";
	private static final Logger LOG = Logger.getLogger(TopkWordCount.class.getName());

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

	/**
	 * Ce programme permet le passage d'une valeur k en argument de la ligne de commande.
	 */
	public static void main(String[] args) throws Exception {
		// Borne 'k' du topk
		int k = 10;

		try {
			// Passage du k en argument ?
			if (args.length > 0) {
				k = Integer.parseInt(args[0]);

				// On contraint k à valoir au moins 1
				if (k <= 0) {
					LOG.warning("k must be at least 1, " + k + " given");
					k = 1;
				}
			}
		} catch (NumberFormatException e) {
			LOG.severe("Error for the k argument: " + e.getMessage());
			System.exit(1);
		}

		Configuration conf = new Configuration();
		conf.setInt("k", k);

		Job job = new Job(conf, "wordcount");

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