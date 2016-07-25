import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.*;
// import org.apache.hadoop.mapred.FileOutputFormat;
// org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Test1 {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		String handles = "", tags = "";
		
		for(int i=0;i<args.length;i++) {
			if (args[i].contains("@")) {
				int index = args[i].indexOf("@");
				String[] temp = args[i].split("@");
				//handles.concat(temp[1].concat(","));
				handles += temp[1] + ",";
			}
			if (args[i].contains("#")) {
				int index = args[i].indexOf("#");
				String[] temp = args[i].split("#");
//				tags.concat(temp[1].concat(","));
				tags += temp[1] + ",";
			}
		}
		
		
		Configuration conf = new Configuration();
		conf.set("handles", handles);
		conf.set("tags", tags);

		Job jobAccess = Job.getInstance(conf);
		jobAccess.setJarByClass(Test1.class); // this class’s name

		String inFile = "/dataset";
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status_list = fs.listStatus(new Path(inFile));
		if (status_list != null) {
			for (FileStatus status : status_list) {
				FileInputFormat.addInputPath(jobAccess, status.getPath());
				// noDoc = noDoc + 1;
			}
		}

		String outFileAccess = "/result";
		FileOutputFormat.setOutputPath(jobAccess, new Path(outFileAccess)); // output
																			// path

		jobAccess.setMapperClass(Test1Mapper.class); // mapper class
		// jobAccess.setCombinerClass(Feature1AccessReducer.class); // optional
		jobAccess.setReducerClass(Test1Reducer.class); // reducer class
		jobAccess.setOutputKeyClass(Text.class); // the key your reducer outputs
		jobAccess.setOutputValueClass(Text.class); // the value
		jobAccess.waitForCompletion(true);
	

	}
}

