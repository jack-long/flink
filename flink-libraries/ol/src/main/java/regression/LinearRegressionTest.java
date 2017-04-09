package regression;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class LinearRegressionTest {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.getRequired("input");

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // operate in Event-time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // create a checkpoint every 5 seconds
        env.enableCheckpointing(5000);
        // try to restart 60 times with 10 seconds delay (10 Minutes)
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(10, TimeUnit.SECONDS)));

        // start the data generator
        DataStream<Tuple2<ArrayList<Double>, Double>> dataStream = env.readTextFile(input)
                .map(new VectorExtractor());

        /**
         *
         * Reference FlinkML & Spark MLlib
         */
        LinearRegression lr = new LinearRegression(0.1, 2);
        /**
         * The fit() method returns the Estimator, the model.
         */
        DataStream<ArrayList<Double>> model = lr.fit(dataStream);
        model.print();
        /**
         * The predict() method returns the prediction.
         */
//        DataStream<Double> predictions = lr.predict(dataStream, model);
        /**
         * Another approach.
         */
        // DataStream<Double> predictions = lr.fitPredict(dataStream);


//        predictions.print();

        // run the prediction pipeline
        env.execute("Linear predictor");
    }

    /**
     * Convert String to Tuple2<ArrayList<Double>, Double>
     */
    public static class VectorExtractor implements MapFunction<String, Tuple2<ArrayList<Double>, Double>> {
        @Override
        public Tuple2<ArrayList<Double>, Double> map(String s) throws Exception {
            String[] elements = s.split(",");
            ArrayList<Double> doubleElements = new ArrayList<>(elements.length - 1);
            for (int i = 0; i < elements.length - 1; i++) {
                doubleElements.add(new Double(elements[i]));
            }
            return new Tuple2<>(doubleElements, new Double(elements[elements.length - 1])) ;
        }
    }

}
