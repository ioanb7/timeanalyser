import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TimeAnalysis  {

	public static class MapClass extends Mapper<LongWritable, Text,LongWritable, LongWritable > {
		
		public void map (LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
			Date d = null;
			try {
				d = sdf.parse("01/01/2017");
			}catch(ParseException e) {
				
			}
			
			String line = value.toString();
			String[] data = line.split(",");
			if(data.length > 4) {
				String datetimeS = data[2];
				String userIdS = data[3];
				
				if(datetimeS.length() > 7 && userIdS.length() > 7) {
					try {
						long datetime = Long.parseLong(datetimeS);
						long userId = Long.parseLong(userIdS);
						datetime = datetime - d.getTime();
					
						context.write(new LongWritable(userId), new LongWritable(datetime));
					}catch(java.lang.NumberFormatException e) {
						
					}
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<LongWritable, LongWritable,LongWritable,LongWritable> {

		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			//do reduce processing - write out the new key and value
			ArrayList<Long> times = new ArrayList<>();
			times.add(0L);
			
			Iterator<LongWritable> it = values.iterator();
			while (it.hasNext()) {
				long val = it.next().get();
				times.add(val);
			}
			
			if(times.size() > 1) {
				Collections.sort(times);
				
				long total = 0;
				for(int i = 1; i < times.size();i++) {
					total += times.get(i) - times.get(i-1);
				}
				
				long val = total / times.size() / 1000;
				if(val != 0) {
					context.write(key, new LongWritable(val));
				}
			}
			
		}
	}
	

	public static void deletePreviousOutput(Configuration conf, Path path)  {

		try {
			FileSystem hdfs = FileSystem.get(conf);
			hdfs.delete(path,true);
		}
		catch (IOException e) {
			//ignore any exceptions
		}
	}
	
	public static void main(String[] args) throws Exception {
      Path in = new Path(args[0]);
      Path out = new Path(args[1]);
      
      Configuration conf = new Configuration();

      deletePreviousOutput(conf, out);
      
		//set any configuration params here. eg to say that the key and value are comma
		//separated in the input data add:
		//conf.set  ("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

		Job job = Job.getInstance(conf);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapperClass(MapClass.class);

		//uncomment the following line if specifying a combiner
		//job.setCombinerClass(Reduce.class);
		
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJarByClass(TimeAnalysis.class);
		job.submit();
		
	}
}
