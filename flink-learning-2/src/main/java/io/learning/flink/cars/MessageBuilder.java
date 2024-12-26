package io.learning.flink.cars;

public class MessageBuilder {

    private String tableName;
    private DataOperator operator;
    private String[] dataArr;

    public MessageBuilder setTableName(String tableName){
        this.tableName = tableName;
        return this;
    }

    public MessageBuilder setOperator(DataOperator operator){
        this.operator = operator;
        return this;
    }
    public MessageBuilder setDataArr(String[] dataArr){
        this.dataArr = dataArr;
        return this;
    }

    public Message build(){
        if (tableName == null || operator == null) {
            throw new IllegalArgumentException("Either tableName or operator must be specified");
        }
        Message message = new Message();
        message.setTableName(tableName);
        message.setOperator(operator);
        message.setDataArr(dataArr);
        return message;
    }

}
