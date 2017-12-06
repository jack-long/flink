package regression;


import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;

public class RegressionData {

    public List<Double> values;
    public Double timestamp;
    public Double label;
    /** useful for debug */
    public long id;

    /**
     * Required no-arg constructor to be as POJO.
     */
    public RegressionData() {}

    public RegressionData(long id, List<Double> values){
        this(id, values, null, null);
    }

    public RegressionData(long id, List<Double> values, Double label){
        this(id, values, label, null);
    }

    public RegressionData(long id, List<Double> values, Double label, Double timestamp){
        this.id = id;
        this.values = values;
        this.label = label;
        this.timestamp = timestamp;
    }

    public int getDimension(){
        return values.size();
    }

    public String toString(){
        return "id: " + id + ", values: " + values + ", label: " + label + ", timestamp: " + timestamp;
    }
}
