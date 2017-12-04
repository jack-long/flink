package regression;


import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;
import java.util.Locale;

public class RegressionData {
    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC(); // TODO: is this necessary?

    public List<Double> values; // TODO: how about array?
    public Double timestamp;
    public Double label;
    /** useful for debug */
    public long id;

    public RegressionData() {}; // TODO: consider the construction

    public RegressionData(long id, List<Double> values, Double label, Double timestamp){
        this.id = id;
        this.values = values;
        this.label = label;
        this.timestamp = timestamp;
    }

    public int getDimention(){
        return values.size();
    }

    public String toString(){
        return "id: " + id + "; values: " + values + "; label: " + label + "; timestamp: " + timestamp;
    }
}
