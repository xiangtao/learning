package io.learning.flink.cars;

/**
 * canel里数据更新操作
 */
public enum  DataOperator {

    INSERT("I"),
    UPDATE("U"),
    DELETE("D");

    private String alise;

    private DataOperator(String alise) {
        this.alise = alise;
    }


    public String getAlise(){
        return this.alise;
    }


    public static DataOperator aliseValueOf(String alise){
        if(alise.equalsIgnoreCase(INSERT.getAlise())){
            return INSERT;
        }else if(alise.equalsIgnoreCase(UPDATE.getAlise())){
            return UPDATE;
        }else if(alise.equalsIgnoreCase(DELETE.getAlise())){
            return DELETE;
        }
        return null;
    }
}
