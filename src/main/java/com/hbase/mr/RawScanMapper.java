package com.hbase.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashSet;


public class RawScanMapper extends TableMapper<Text, Text>{


	private static HashSet<String> cellkvs = new HashSet<String>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		cellkvs.clear();
		String key="";
		int count = 0;
		for (Cell cell:value.rawCells()) {
			key = Bytes.toStringBinary(cell.getRow())+"-"+ Bytes.toStringBinary(cell.getFamily())+"-"+ Bytes.toStringBinary(cell.getQualifier());
			if(cellkvs.contains(key)){
				count++;
			}else{
				cellkvs.add(key);
			}
		}
		if(count > 0) {
			context.write(new Text(key),new Text(String.valueOf(count)));
		}
	}
}
