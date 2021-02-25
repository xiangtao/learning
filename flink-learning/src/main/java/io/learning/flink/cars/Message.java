package io.learning.flink.cars;

public class Message {

    private String tableName;
    private DataOperator operator;
    private String[] dataArr;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public DataOperator getOperator() {
        return operator;
    }

    public void setOperator(DataOperator operator) {
        this.operator = operator;
    }

    public String[] getDataArr() {
        return dataArr;
    }

    public void setDataArr(String[] dataArr) {
        this.dataArr = dataArr;
    }

    public static MessageBuilder builder(){
        return new MessageBuilder();
    }
}
