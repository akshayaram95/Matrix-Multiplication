package com.utdallas.bigdata.assignment;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MatrixValueWritable implements Writable{
	private String matrixName = new String();
	private long i;
	private long j;
	private long val;
	
	public MatrixValueWritable(){
	}
	
	public MatrixValueWritable(MatrixValueWritable writable){
		this.matrixName = writable.matrixName;
		this.i = writable.i;
		this.j = writable.j;
		this.val = writable.val;
	}
	
	public void write(DataOutput out) throws IOException{
		out.writeLong(i);
		out.writeLong(j);
		out.writeLong(val);
		out.writeChars(matrixName);
	}

	public void readFields(DataInput in) throws IOException{
		i = in.readLong();
		j = in.readLong();
		val = in.readLong();
		matrixName = in.readLine();
	}

	public String getMatrixName() {
		return matrixName;
	}

	public void setMatrixName(String matrixName) {
		this.matrixName = matrixName;
	}

	public long getI() {
		return i;
	}

	public void setI(long i) {
		this.i = i;
	}

	public long getJ() {
		return j;
	}

	public void setJ(long j) {
		this.j = j;
	}

	public long getVal() {
		return val;
	}

	public void setVal(long val) {
		this.val = val;
	}
}