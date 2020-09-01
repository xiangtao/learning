package io.learning.thrift.server;

import io.learning.thrift.service.HelloService;
import org.apache.thrift.TException;

public class HelloServiceImpl implements HelloService.Iface {
  @Override
  public String helloString(String s) throws TException {
    return "resp: " + s;
  }
}
