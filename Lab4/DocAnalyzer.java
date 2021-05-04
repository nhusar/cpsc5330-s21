import java.io.IOException;
import java.lang.Math;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DocAnalyzer {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

    private static final Pattern wordPattern = Pattern.compile("[a-z]+");

    private final Text outDocId = new Text();
    private final Text outToken = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      final String fileName = ((FileSplit) context.getInputSplit()).getPath().getName().replaceAll(".txt", "");
      outDocId.set(fileName);
      for (Matcher match = wordPattern.matcher(value.toString().toLowerCase()); match.find(); ) {
        outToken.set(match.group());
        context.write(outDocId, outToken);
      }
    }
  }

  public static class TokenCategoryReducer extends Reducer<Text, Text, Text, Text> {

    private final HashSet<Srting> stopwords = new HashSet<>();
    private final Text outStats = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int countTokens = 0;
      int countStopwords = 0;
      int countWords = 0;
      int totalWordLen = 0;
      String wordLongest = "";
      HashSet<String> uniqueTerms = new HashSet<>();
      for (Text val : values) {
        final String token = val.toString();
        ++countTokens;
        if (stopwords.contains(token)) {
          ++countStopwords;
        } else {
          ++countWords;
          totalWordLen += token.length();
          uniqueTerms.add(token);
          if (token.length() > wordLongest.length()) {
            wordLongest = token;
          }
        }
      }
      outStats.set(String.format("%s\t%d\t%d\t%d\t%.4f\t%s",
        key, countTokens, countStopwords, uniqueTerms.size(),
        totalWordLen / Math.max(1.0, countWords), wordLongest));
      context.write(key, outStats);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "DocAnalyzer");

    job.setJarByClass(DocAnalyzer.class);

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
