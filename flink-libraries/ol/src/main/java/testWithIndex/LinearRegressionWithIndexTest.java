package testWithIndex;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

public class LinearRegressionWithIndexTest {
    public static void main(String[] args) throws Exception {
        // handle input parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.getRequired("input");

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // operate in Event-time
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // create a checkpoint every 5 seconds
//        env.enableCheckpointing(5000);
        // try to restart 60 times with 10 seconds delay (10 Minutes)
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(10, TimeUnit.SECONDS)));

        // start the data generator
        DataStream<ArrayList<Double>> dataStream = env.readTextFile(input)
                .map(new VectorExtractor());  // partitioned
//        dataStream.writeAsText("output", OVERWRITE).setParallelism(1);

        // Reference FlinkML & Spark MLlib
        LinearRegressionWithIndex lr = new LinearRegressionWithIndex();

        // The fit() method returns the Estimator, the model.
        DataStream<Tuple4<ArrayList<Double>, Double, Integer, Integer>> model = lr.fit(dataStream);
        model.writeAsText("output/output_drift_with_index", OVERWRITE); //.setParallelism(1);

//        DataStream<ArrayList<Double>> pureModel = model.map(new MapFunction<Tuple2<ArrayList<Double>, Double>, ArrayList<Double>>() {
//            @Override
//            public ArrayList<Double> map(Tuple2<ArrayList<Double>, Double> value) throws Exception {
//                return value.f0;
//            }
//        });

        /**
         * The predict() method returns the prediction.
         */
//        DataStream<Tuple2<Double, Double>> predictions = lr.predict(dataStream, pureModel);
//        predictions.writeAsText("output_prediction", OVERWRITE);
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
//    public static class VectorExtractor implements MapFunction<String, Tuple2<ArrayList<Double>, Double>> {
//        @Override
//        public Tuple2<ArrayList<Double>, Double> map(String s) throws Exception {
//            String[] elements = s.split(",");
//            ArrayList<Double> doubleElements = new ArrayList<>(elements.length - 1);
//            for (int i = 0; i < elements.length - 1; i++) {
//                doubleElements.add(new Double(elements[i]));
//            }
//            return new Tuple2<>(doubleElements, new Double(elements[elements.length - 1])) ;
//        }
//    }
    public static class VectorExtractor implements MapFunction<String, ArrayList<Double>> {
        @Override
        public ArrayList<Double> map(String s) throws Exception {
            String[] elements = s.split(",");
            ArrayList<Double> doubleElements = new ArrayList<>(elements.length);
            for (int i = 0; i < elements.length; i++) {
                doubleElements.add(new Double(elements[i]));
            }
            return new ArrayList<>(doubleElements) ;
        }
    }

}
