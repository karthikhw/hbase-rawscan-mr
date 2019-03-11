package com.hbase.mr;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class RawScanMapper extends TableMapper<Text, Text>{

	public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		int versions = value.rawCells().length;
		if(versions > 1){
		String rowKey = Bytes.toStringBinary(value.getRow());
		versions = versions - 1;
        context.write(new Text(rowKey),new Text(String.valueOf(versions)));
		}
	}
}
