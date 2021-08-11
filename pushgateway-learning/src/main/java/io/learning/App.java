package io.learning;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class App {

  public static void main(String[] args) {
    try {
      String url = "pushgateway-biflink.koolearn.com:80";
      CollectorRegistry registry = CollectorRegistry.defaultRegistry;
      Gauge guage = Gauge.build("xt_custom_metric", "This is my custom metric.").create();
      guage.set(1000);
      guage.register(registry);
      PushGateway pg = new PushGateway(url);
      Map<String, String> groupingKey = new HashMap<String, String>();
      groupingKey.put("instance", "my_instance");
      pg.pushAdd(registry, "xt_my_job", groupingKey);
      //pg.push(registry,"xt_my_job", groupingKey);

      main2(null);

      System.out.println("aaaaaaaaa bbbbbbbb");

      Thread.sleep(1000);
      guage.set(1001);
      pg.push(registry, "xt_my_job", groupingKey);

      System.out.println("aaaaaaaaa bbbbbbbb");


    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main2(String[] args) {
    try{
      String url = "pushgateway-biflink.koolearn.com:80";
      CollectorRegistry registry = new CollectorRegistry();
      Gauge guage = Gauge.build("my_custom_metric2", "This is my custom metric.").create();
      String date = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss").format(new Date());
      guage.set(50);
      guage.register(registry);
      PushGateway pg = new PushGateway(url);
      Map<String, String> groupingKey = new HashMap<String, String>();
      groupingKey.put("instance", "my_instance");
      pg.pushAdd(registry, "xt_my_job", groupingKey);
      System.out.println("222 aaaaaaaaa bbbbbbbb");
    } catch (Exception e){
      e.printStackTrace();
    }
  }
}
