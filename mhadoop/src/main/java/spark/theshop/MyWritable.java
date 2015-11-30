package spark.theshop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import scala.Tuple2;

public class MyWritable implements Writable, Serializable{
	  private String _1;
	  private Double _2;

	  public MyWritable(Tuple2<String, Double> data){
	    _1 = data._1;
	    _2 = data._2;
	  }

	  public Tuple2<String, Double> get(){
	    return new Tuple2<String, Double>(_1, _2);
	  }

	  public void readFields(DataInput in) throws IOException {
	    _1 = WritableUtils.readString(in); // in. //(in.readInt());
	    _2 = in.readDouble();
	  }

	  public void write(DataOutput out) throws IOException {
		  out.writeChars(_1); // out.writeInt(_1);
		  out.writeDouble(_2);
	  }

	
	}