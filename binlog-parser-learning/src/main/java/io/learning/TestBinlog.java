package io.learning;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import java.io.IOException;

public class TestBinlog {

  public static void main(String[] args) throws IOException {
    BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "root");
    EventDeserializer eventDeserializer = new EventDeserializer();
    //时间反序列化的格式
//        eventDeserializer.setCompatibilityMode(
//                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
//                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
//        );
    client.setEventDeserializer(eventDeserializer);
    client.setServerId(654321);
    client.setHeartbeatInterval(500);
    client.registerEventListener(new BinaryLogClient.EventListener() {
      @Override
      public void onEvent(Event event) {
        EventHeader header = event.getHeader();
        EventType eventType = header.getEventType();
        System.out.println("监听的事件类型:" + eventType);
        System.out.println("header:" + header);
        System.out.println("data:" + event.getData());
        if (EventType.isWrite(eventType)) {
          //获取事件体
          WriteRowsEventData data = event.getData();
          System.out.println(data);
        } else if (EventType.isUpdate(eventType)) {
          UpdateRowsEventData data = event.getData();
          System.out.println(data);
        } else if (EventType.isDelete(eventType)) {
          DeleteRowsEventData data = event.getData();
          System.out.println(data);
        }
      }
    });
    client.connect();
  }
}
