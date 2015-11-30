package spark;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import scala.Tuple2;
//

public class OrderWritable implements Writable {
	private String orderNum;
	private List<OrderInfoModel> orderInfo;
	
	
	public OrderWritable(Tuple2<String, List<OrderInfoModel>> data) {
		orderNum = data._1;
		orderInfo = data._2();
	}
	
	public Tuple2<String, List<OrderInfoModel>> get() {
		return new Tuple2<String, List<OrderInfoModel>>(orderNum, orderInfo);
	}
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		orderNum = WritableUtils.readString(in);
		//ArrayWritable _2Writable = new ArrayWritable((Class<? extends Writable>) OrderInfoModel.class);
		//_2Writable.readFields(in);
		//orderInfo = _2Writable;
	}

	public void write(DataOutput out) throws IOException {
		Text.writeString(out, orderNum);
	    //ArrayWritable _2Writable = new ArrayWritable(orderInfo);
	   // _2Writable.write(out);
		
	}

}
