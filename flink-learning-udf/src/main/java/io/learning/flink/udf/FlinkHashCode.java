package io.learning.flink.udf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class FlinkHashCode extends ScalarFunction {

  private int factor = 12;

  /**
   * Setup method for user-defined function. It can be used for initialization work.
   * By default, this method does nothing.
   */
  public void open(FunctionContext context) throws Exception {
    // do nothing
  }

  public String eval(String s) {
    return s.hashCode() * factor + "";
  }

}
