package com.utdallas.bigdata.assignment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MatrixMultiplication {
	public static class MapClass1 extends Mapper<LongWritable, Text, Text, MatrixValueWritable>{
		MatrixValueWritable matrixValue = new MatrixValueWritable();
		Text word = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String input[] = value.toString().replaceAll("[^A-Za-z0-9,]", "").split(",");
			matrixValue.setMatrixName(input[0]);
;			matrixValue.setI(Long.parseLong(input[1]));
			matrixValue.setJ(Long.parseLong(input[2]));
			matrixValue.setVal(Long.parseLong(input[3]));
			word.set(input[0]);
			context.write(word,matrixValue);
		}
	}
	
	public static class ReducerClass1 extends Reducer<Text,MatrixValueWritable,Text,Text> {
		private Text outputValue = new Text();
		public void reduce(Text key, Iterable<MatrixValueWritable> values, Context context) throws IOException, InterruptedException{
			String output = "";
			for(MatrixValueWritable matrix:values){
				output+= matrix.getI()+","+matrix.getJ()+","+matrix.getVal()+";";
			}
			output = output.substring(0, output.length()-1);
			outputValue.set(output);
			context.write(key, outputValue);
		}
	}
	
	public static class MapClass2 extends Mapper<LongWritable, Text, Text, MatrixValueWritable>{
		MatrixValueWritable matrixValue = new MatrixValueWritable();
		Text word = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String input[] = value.toString().split("\t");
			String matrixName = input[0];
			String elements[] = input[1].split(";");
			long max = -1;
			if(matrixName.contains("A")){
				for(String element:elements){
					String matrixElements[] = element.split(",");
					if(max < Long.parseLong(matrixElements[1])){
						max = Long.parseLong(matrixElements[1]);
					}
				}
			}else{
				for(String element:elements){
					String matrixElements[] = element.split(",");
					if(max < Long.parseLong(matrixElements[0])){
						max = Long.parseLong(matrixElements[0]);
					}
				}
			}
			for(String element:elements){
				String matrixElements[] = element.split(",");
				matrixValue.setMatrixName(matrixName);
	;			matrixValue.setI(Long.parseLong(matrixElements[0]));
				matrixValue.setJ(Long.parseLong(matrixElements[1]));
				matrixValue.setVal(Long.parseLong(matrixElements[2]));
				if(matrixName.contains("A")){
					for(long k=0;k<=max;k++){
						String tempKey = matrixValue.getI()+","+k;
						word.set(tempKey);
						context.write(word, matrixValue);
					}
				}else{
					for(long i=0; i<=max; i++){
						String tempKey = i+","+matrixValue.getJ();
						word.set(tempKey);
						context.write(word, matrixValue);
					}
				}
			}
		}
	}
	
	public static class ReducerClass2 extends Reducer<Text,MatrixValueWritable,Text,Text> {
		private Text outputValue = new Text();
		public void reduce(Text key, Iterable<MatrixValueWritable> values, Context context) throws IOException, InterruptedException{
			HashMap<Long,Long> matrixA = new HashMap<Long,Long>();
			HashMap<Long,Long> matrixB = new HashMap<Long,Long>();
			for(MatrixValueWritable val:values){
				if(val.getMatrixName().contains("A")){
					matrixA.put(val.getJ(), val.getVal());
				}else{
					matrixB.put(val.getI(), val.getVal());
				}
			}
			long sum = 0;
			for(long row:matrixA.keySet()){
				if(matrixB.containsKey(row)){
					sum += (matrixA.get(row)*matrixB.get(row));
				}
			}
			if(sum > 0){
				outputValue.set(String.valueOf(sum));
				context.write(key, outputValue);
			}
		}
	}
		
	public static class GroupComparator extends Text.Comparator {
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
			int cmp = super.compare(b1, s1, l1, b2, s2, l2);
			return cmp;
		}
	}
	
	public static class SortComparator extends Text.Comparator {
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
			int cmp = super.compare(b1, s1, l1, b2, s2, l2);
			return cmp;
		}
	}
	
	public static void main(String[] args) throws Exception {
	  	Configuration conf = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  	if (otherArgs.length != 3) {
	  	System.err.println("Usage: MatrixMultiplication <in> <temp> <out>");
	  	System.exit(2);
	  	}
	  	Job job1 = new Job(conf, "MatrixMultiplication1");
	  	job1.setJarByClass(MatrixMultiplication.class);
	  	job1.setMapperClass(MapClass1.class);
	  	job1.setReducerClass(ReducerClass1.class);
	  	job1.setNumReduceTasks(2);
	  	job1.setOutputKeyClass(Text.class);
	  	job1.setOutputValueClass(Text.class);
	  	job1.setMapOutputKeyClass(Text.class);
	  	job1.setMapOutputValueClass(MatrixValueWritable.class);
	  	FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
	  	FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
	  	job1.waitForCompletion(true);
	  	
	  	//job2
	  	Job job2 = new Job(conf, "MatrixMultiplication2");
	  	job2.setJarByClass(MatrixMultiplication.class);
	  	job2.setMapperClass(MapClass2.class);
	  	job2.setReducerClass(ReducerClass2.class);
	  	job2.setNumReduceTasks(4);
	  	job2.setOutputKeyClass(Text.class);
	  	job2.setOutputValueClass(Text.class);
	  	job2.setMapOutputKeyClass(Text.class);
	  	job2.setMapOutputValueClass(MatrixValueWritable.class);
	  	FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
	  	FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
	  	System.exit(job2.waitForCompletion(true) ? 0 : 1);
  	}


}
