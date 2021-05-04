import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordAnalyzer {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

      private Text outToken = new Text();
      private Text outCategory = new Text();

      private String wordCategory(String word) {
	  return word.charAt(0) > 'm' ? "late" : "early";
      }

      public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName().replaceAll(".txt", "");
      outToken.set(fileName);
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
	  String nextToken = itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
	  if (nextToken.length() > 0) {
	      outCategory.set(wordCategory(nextToken));
	      context.write(outToken, outCategory);
	  }
      }
    }
  }

  public static class TokenCategoryReducer
       extends Reducer<Text,Text,Text,Text> {

    private Text outStats = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
	int numEarly = 0;
	int numLate = 0;
	int numUnknown = 0;
	for (Text val : values) {
	    if (val.toString().equals("early")) {
		numEarly += 1;
	    } else if (val.toString().equals("late")) {
		numLate += 1;
	    } else {
		numUnknown += 1;
	    }
	}
	outStats.set(numEarly + "\t" + numLate + "\t" + numUnknown);
	context.write(key, outStats);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    Job job = Job.getInstance(conf, "WordAnalyzer");
    
    job.setJarByClass(WordAnalyzer.class);
    
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setReducerClass(TokenCategoryReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
