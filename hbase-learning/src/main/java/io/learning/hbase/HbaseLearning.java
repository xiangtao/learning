package io.learning.hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseLearning {

  public static void main(String[] args) {
    try {
      Configuration conf = new Configuration();
      // 1.4版本推荐的初始化方式，new HTable方式已被废弃
      Connection connection = ConnectionFactory.createConnection(conf);
      Table table = connection.getTable(TableName.valueOf("test"));

      byte[] rowKey = Bytes.toBytes("key_yiz96");
      byte[] columnFamily = Bytes.toBytes("cf1");
      byte[] qualifier = Bytes.toBytes("name");
      byte[] value = Bytes.toBytes("YiZheng");

      Put put = new Put(rowKey);
      put.addColumn(columnFamily, qualifier, value);
      table.put(put);

      table.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
