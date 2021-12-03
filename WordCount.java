
package org.apache.hadoop.examples;

import java.util.HashTable;
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
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> 
    {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String[] badWords = [ "he", "she", "they", "the", "a", "an", "are","you", "of", "is", "and", "or", "was", "them"];
        private int topN = 5;
        // private HashTable <String, Long> wordCount = new HashTable();
        // private TreeMap<Long, String> map;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) 
            {
                String token = itr.nextToken();
                boolean continuer = false;

                for(String word : badWords) {
                    if(word.equals(token)) {
                        continuer = true;
                        break;
                    }
                }
                if(continuer) {
                    continue;
                } else {        
                    word.set(itr.nextToken());
                    context.write(word, one);
                }
            }    
        }
    }   

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private Map<String, Integer> allWords = new HashMap<>();
        private Map<Integer, String> top5 = new TreeMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            int timesAppeared = 0; int counter=0;
            for (Text val : values) {

                timesAppeared = 1;
                if(allWords.containsKey(val.toString())) {
                    timesAppeared = allWords.get(val.toString()) + 1;
                }
                allWords.put(val.toString(), timesAppeared)
            }

            for(Map.Entry<String, Integer> putSortedValue : allWords.entrySet()) {
                allWords.put(putSortedValue.getValue(), putSortedValue.getKey())
            }


            for(Map.Entry<String, Integer> writeSortedValue : top5.entrySet()) {
                if(count == 5) {
                    break;
                }

                context.write(new LongWritable(writeSortedValue.getKey()), new Text(writeSortedValue.getEntry())); //only write the top 5 words
                counter++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1); // only 1 reducer
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}