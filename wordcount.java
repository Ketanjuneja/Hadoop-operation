package demo.mr;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.StringTokenizer;

//driver class
public class wordCount {

	 public static class MyMap extends Mapper<LongWritable,Text,Text,IntWritable>
	 { 
		  
		

       		 public void map(LongWritable k,Text v,Context con)throws IOException, InterruptedException 
	  {
			   
			  String line=v.toString();
			  StringTokenizer t= new StringTokenizer(line);
			  while(t.hasMoreElements())
			  {
				  String word =t.nextToken();
				  con.write(new  Text(word),new IntWritable(1));
				  
			  }
			 
			 
			 
	   }
	 }
     
      public static class MyRed extends Reducer<Text,IntWritable,Text,IntWritable>
      {
    	  public void reduce(Text k,Iterable<IntWritable> vlist , Context con)
    	  throws IOException,InterruptedException
    	  {
    		  int tot=0;
    		  for(IntWritable v:vlist)
    			  tot+=v.get();
    		  con.write(k, new IntWritable(tot));
    		  
    	  }
    	  
    	  
      }
      
      
      public static void main(String[] args) throws Exception
      {
    	  Configuration c = new Configuration();
    	  Job j=new Job(c,"MyFirstJob");
    	  j.setJarByClass(wordCount.class);
    	  j.setMapperClass(MyMap.class);
    	//  j.setNumReduceTasks(0);
    	  j.setReducerClass(MyRed.class);
    	  j.setOutputKeyClass(Text.class);//mapper output key
    	  j.setOutputValueClass(IntWritable.class);//mapper output value
    	  Path p1=new Path(args[0]);
    	  Path p2=new Path(args[1]);
    	  
    	  FileInputFormat.addInputPath(j, p1);
    	  FileOutputFormat.setOutputPath(j, p2);
    	  System.exit(j.waitForCompletion(true)? 0:1);
    	  
      }
}
