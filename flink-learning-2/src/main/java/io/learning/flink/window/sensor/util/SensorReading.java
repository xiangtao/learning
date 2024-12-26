package io.learning.flink.window.sensor.util;

public class SensorReading {

    // id of the sensor
    public String id;
    // timestamp of the reading
    public long timestamp;
    // temperature value of the reading
    public double temperature;

    /**
     * Empty default constructor to satify Flink's POJO requirements.
     */
    public SensorReading() { }

    public SensorReading(String id, long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String toString() {
        return "(" + this.id + ", " + this.timestamp + ", " + this.temperature + ")";
    }
}
