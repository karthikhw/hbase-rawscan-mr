package com.hbase.mr;

import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.hbase.Cell;
    import org.apache.hadoop.hbase.HBaseConfiguration;
    import org.apache.hadoop.hbase.TableName;
    import org.apache.hadoop.hbase.client.*;
    import org.apache.hadoop.hbase.util.Bytes;

    import java.io.IOException;
    import java.util.HashSet;


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
      HashSet<String> keys = new HashSet<String>();
      for (Cell cell:result.rawCells()) {
        String key = Bytes.toStringBinary(cell.getRow())+"-"+ Bytes.toStringBinary(cell.getFamily())+"-"+ Bytes.toStringBinary(cell.getQualifier());
        System.out.println("keys: "+key);
        if(keys.contains(key)){
          System.out.println("Multiple Versions: "+key);
        }else{
          keys.add(key);
        }
      }
    }
  }
}
