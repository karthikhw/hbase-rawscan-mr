package com.hbase.mr;

import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.hbase.HBaseConfiguration;
    import org.apache.hadoop.hbase.TableName;
    import org.apache.hadoop.hbase.client.*;
    import org.apache.hadoop.hbase.util.Bytes;

    import java.io.IOException;


public class RawScanner {

  public static void main(String args[]) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf(args[0]));
    Scan scan = new Scan();
    scan.setCaching(5000);
    scan.setMaxVersions();
    scan.setRaw(true);
    ResultScanner scanner = table.getScanner(scan);
    for (Result result:scanner) {
      int version = result.rawCells().length;
      if(version>1){
        System.out.println("Key contains multiple Versions : "+Bytes.toStringBinary(result.getRow()));
      }
    }
  }
}
